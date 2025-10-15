import os
import requests
import zipfile
import chardet
import pandas as pd
import locale
import time
import json
import shutil
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from datetime import datetime

# Importa as configurações
from config import PASTA_RAIZ_DESTINO, CONFIG_BD, PASTA_DOWNLOADS, PASTA_EXTRAIDOS_TEMP

# --- Funcoes de Suporte ---

def limpar_pastas_temporarias():
    print("Limpando arquivos temporarios de execucoes anteriores...")
    for pasta in [PASTA_DOWNLOADS, PASTA_EXTRAIDOS_TEMP]:
        if os.path.exists(pasta):
            shutil.rmtree(pasta)
            print(f"Pasta removida: {pasta}")

def carregar_manifesto(caminho):
    if os.path.exists(caminho):
        with open(caminho, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def salvar_manifesto(caminho, dados):
    with open(caminho, 'w', encoding='utf-8') as f:
        json.dump(dados, f, indent=4)

def contar_linhas_arquivo(caminho, encoding='utf-8'):
    try:
        with open(caminho, 'r', encoding=encoding, errors='replace') as f:
            return sum(1 for _ in f)
    except Exception:
        return -1

# --- Funcoes Principais do ETL ---

def fase_planejamento(ano, mes, caminho_manifesto):
    print("\n--- Iniciando planejamento ---")
    if os.path.exists(caminho_manifesto):
        print("Arquivo de controle encontrado, continuando processo...")
        return carregar_manifesto(caminho_manifesto)

    print("Buscando lista de arquivos no site...")
    site_receita = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{ano}-{mes}/"
    manifesto = {}
    try:
        response = requests.get(site_receita)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        for link_tag in soup.find_all('a'):
            href = link_tag.get('href')
            if href and href.endswith('.zip'):
                manifesto[href] = {
                    "url": site_receita + href, "status_download": "pendente",
                    "status_extracao": "pendente", "status_correcao": "pendente",
                    "status_carga": "pendente", "caminho_zip": os.path.join(PASTA_DOWNLOADS, href)
                }
        salvar_manifesto(caminho_manifesto, manifesto)
        print(f"Arquivos encontrados: {len(manifesto)}")
        return manifesto
    except requests.exceptions.RequestException as e:
        print(f"Erro: Falha ao acessar o site da Receita. {e}")
        return None

def fase_download(manifesto, caminho_manifesto):
    print("\n--- Iniciando download dos arquivos ---")
    for arq, dados in manifesto.items():
        if dados["status_download"] == "pendente":
            sucesso = False
            for tentativa in range(3):
                print(f"Baixando {arq} (tentativa {tentativa + 1}/3)...")
                try:
                    response = requests.get(dados["url"], stream=True, timeout=60)
                    response.raise_for_status()
                    with open(dados["caminho_zip"], 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    sucesso = True
                    break
                except requests.exceptions.RequestException:
                    print("  Falha no download, aguardando 5 segundos...")
                    time.sleep(5)
            
            dados["status_download"] = "sucesso" if sucesso else "falhou"
            salvar_manifesto(caminho_manifesto, manifesto)

def fase_extracao(manifesto, caminho_manifesto):
    print("\n--- Iniciando descompactacao ---")
    for arq, dados in manifesto.items():
        if dados["status_download"] == "sucesso" and dados["status_extracao"] == "pendente":
            print(f"Extraindo: {arq}")
            try:
                with zipfile.ZipFile(dados["caminho_zip"], 'r') as zf:
                    zf.extractall(path=PASTA_EXTRAIDOS_TEMP)
                    dados["status_extracao"] = "sucesso"
                    dados["arquivos_extraidos"] = [os.path.join(PASTA_EXTRAIDOS_TEMP, nome) for nome in zf.namelist()]
            except zipfile.BadZipFile:
                print(f"  Erro: Arquivo {arq} corrompido. Sera baixado novamente na proxima execucao.")
                dados["status_extracao"] = "falhou"
                dados["status_download"] = "pendente"
            salvar_manifesto(caminho_manifesto, manifesto)

def fase_correcao(manifesto, caminho_manifesto, pasta_destino):
    print("\n--- Iniciando correcao e validacao dos arquivos ---")
    for dados in manifesto.values():
        if dados["status_extracao"] == "sucesso" and dados["status_correcao"] == "pendente":
            caminho_original = dados["arquivos_extraidos"][0]
            nome_arquivo = os.path.basename(caminho_original)
            print(f"Corrigindo: {nome_arquivo}")
            try:
                with open(caminho_original, 'rb') as f_raw:
                    encoding = chardet.detect(f_raw.read(100000))['encoding']
                
                linhas_originais = contar_linhas_arquivo(caminho_original, encoding)

                nome_base, ext = os.path.splitext(nome_arquivo)
                nome_corrigido = f"{nome_base}__corrigido{ext}"
                caminho_corrigido = os.path.join(pasta_destino, nome_corrigido)
                dados["caminho_corrigido"] = caminho_corrigido

                with open(caminho_original, 'r', encoding=encoding, errors='replace') as f_in, \
                        open(caminho_corrigido, 'w', encoding='utf-8-sig', newline='') as f_out:
                    f_out.writelines(f_in)
                
                linhas_corrigidas = contar_linhas_arquivo(caminho_corrigido, 'utf-8-sig')
                
                # Validacao
                if linhas_originais == linhas_corrigidas and linhas_originais != -1:
                    print("  Validacao de linhas: ok")
                    dados["status_correcao"] = "sucesso"
                    dados["validacao_linhas"] = f"{linhas_corrigidas}"
                else:
                    print(f"  Erro de validacao: {linhas_originais} linhas (original) vs {linhas_corrigidas} (corrigido)")
                    dados["status_correcao"] = "falhou"
            except Exception as e:
                print(f"  Erro critico na correcao: {e}")
                dados["status_correcao"] = "falhou"
            salvar_manifesto(caminho_manifesto, manifesto)

def fase_carga(manifesto, caminho_manifesto, schema_nome):
    print("\n--- Iniciando carga no banco de dados ---")
    mapa_tabelas = {
        "EMPRECSV": "rfb_empresas", "ESTABELE": "rfb_estabelecimentos", "SOCIOCSV": "rfb_socios",
        "SIMPLES": "rfb_simples", "CNAECSV": "rfb_cnaes", "MOTIV": "rfb_motivos",
        "MUNIC": "rfb_municipios", "NATJU": "rfb_naturezas", "PAIS": "rfb_paises",
        "QUALS": "rfb_qualificacoes"
    }
    try:
        conn_str = f"postgresql+psycopg2://{CONFIG_BD['usuario']}:{CONFIG_BD['senha']}@{CONFIG_BD['host']}:{CONFIG_BD['porta']}/{CONFIG_BD['banco']}"
        engine = create_engine(conn_str)
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_nome}")
    except Exception as e:
        print(f"Erro critico: Falha ao conectar no banco de dados. {e}")
        return

    for dados in manifesto.values():
        if dados["status_correcao"] == "sucesso" and dados["status_carga"] == "pendente":
            caminho_corrigido = dados["caminho_corrigido"]
            nome_arquivo_corrigido = os.path.basename(caminho_corrigido)
            
            nome_base_original = nome_arquivo_corrigido.split('__corrigido')[0]
            partes_nome = nome_base_original.split('.')
            identificador = partes_nome[-1].upper() if len(partes_nome) > 1 else partes_nome[0].upper()
            nome_tabela = mapa_tabelas.get(identificador)

            if not nome_tabela:
                dados["status_carga"] = "ignorada"
                continue
            
            print(f"Carregando {nome_arquivo_corrigido} para a tabela {schema_nome}.{nome_tabela}...")
            try:
                with engine.connect() as connection:
                    with connection.begin():
                        connection.execute(f"TRUNCATE TABLE {schema_nome}.{nome_tabela}")
                
                linhas_no_arquivo = int(dados["validacao_linhas"])
                
                for chunk in pd.read_csv(caminho_corrigido, sep=';', header=None, encoding='utf-8-sig', chunksize=50000, low_memory=False, dtype=str):
                    chunk.to_sql(nome_tabela, engine, schema=schema_nome, if_exists='append', index=False, method='multi')

                # Validacao
                with engine.connect() as connection:
                    registros_no_banco = connection.execute(f"SELECT COUNT(*) FROM {schema_nome}.{nome_tabela}").scalar()
                
                if linhas_no_arquivo == registros_no_banco:
                    print("  Validacao de carga: ok")
                    dados["status_carga"] = "sucesso"
                else:
                    print(f"  Erro de validacao: {linhas_no_arquivo} (arquivo) vs {registros_no_banco} (banco)")
                    dados["status_carga"] = "falhou"
            except Exception as e:
                print(f"  Erro critico na carga: {e}")
                dados["status_carga"] = "falhou"
            salvar_manifesto(caminho_manifesto, manifesto)

def relatorio_final(manifesto):
    print("\n--- Relatorio final ---")
    sucesso = falhas = 0
    for dados in manifesto.values():
        if dados.get('status_carga') == 'sucesso':
            sucesso += 1
        else:
            falhas += 1
    
    print(f"Arquivos com carga finalizada: {sucesso}")
    print(f"Arquivos com falha ou pendentes: {falhas}")
    if falhas > 0:
        print("\nDetalhes dos arquivos com falha:")
        for arq, dados in manifesto.items():
            if dados.get('status_carga') != 'sucesso':
                print(f"- {arq} (Etapa final: {dados.get('status_carga', 'N/A')})")

# --- Execucao Principal ---

if __name__ == "__main__":
    print("--- Iniciando pipeline de ETL da Receita Federal ---")
    
    limpar_pastas_temporarias()

    ano_input = input("Digite o ano para processar (ex: 2025): ")
    mes_input = input("Digite o mes para processar (1-12): ")
    
    try:
        data_alvo = datetime(int(ano_input), int(mes_input), 1)
        locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
    except Exception as e:
        print(f"Erro: Data invalida ou problema de configuracao de idioma. {e}")
        exit()

    ano = data_alvo.strftime("%Y")
    mes_num = data_alvo.strftime("%m")
    mes_nome = data_alvo.strftime("%B").capitalize()
    
    # Montagem dos caminhos
    pasta_destino_final = os.path.join(PASTA_RAIZ_DESTINO, ano, f"{int(mes_num)}. {mes_nome}")
    schema_bd = f"rfb_{ano}{mes_num}"
    caminho_manifesto_final = os.path.join(pasta_destino_final, "manifest.json")

    # Criacao das pastas
    os.makedirs(PASTA_DOWNLOADS, exist_ok=True)
    os.makedirs(PASTA_EXTRAIDOS_TEMP, exist_ok=True)
    os.makedirs(pasta_destino_final, exist_ok=True)
    
    # Execucao das fases do ETL
    manifesto = fase_planejamento(ano, mes_num, caminho_manifesto_final)

    if manifesto:
        fase_download(manifesto, caminho_manifesto_final)
        fase_extracao(manifesto, caminho_manifesto_final)
        fase_correcao(manifesto, caminho_manifesto_final, pasta_destino_final)
        fase_carga(manifesto, caminho_manifesto_final, schema_bd)
        relatorio_final(manifesto)
        
        limpar_pastas_temporarias()
        
        print("\nProcesso concluido.")
    else:
        print("\nProcesso interrompido: Nao foi possivel iniciar o planejamento.")