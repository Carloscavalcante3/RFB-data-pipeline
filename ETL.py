import requests
import pandas
import httplib2
from bs4 import BeautifulSoup, SoupStrainer

#Definição de variáveis
mes = "09"
ano = "2025"
site_receita = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{ano}-{mes}/"

# Extração

def extrair_dados(endpoint):
    response = requests.get(endpoint)
    if response.status_code == 200:
        print(response.text) # Troque o return por este print
        return "OK" # Retorne algo simples por enquanto
    else:
        print(f"Erro ao extrair os dados: {response.status_code}")
        return None

extrair_dados(site_receita)