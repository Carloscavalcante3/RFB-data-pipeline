# PIPELINE ETL PARA BASE DA RECEITA FEDERAL - CNPJ

## 👋 Apresentação
-   Este projeto visa criar uma pipeline de dados, otimizando o processo mensal de Extração, Limpeza e Carregamento **(ETL)** das bases referentes a CNPJ da Receita Federal, facilitando o dia a dia dos analistas do Departamento Interno de Prestação de Contas (DIPC) da Controladoria do Estado de Pernambuco

## 🏗️ Funcionamento
- Monitoramento e Ativação Automática
    * A pipeline opera em modo "vigilante", verificando diariamente o site da Receita Federal pela publicação dos dados do mês de referência.
    * O processo só é iniciado quando os novos arquivos são detectados, evitando execuções desnecessárias.

- Etapa 1: Extração
    * Download: Baixa automaticamente todos os arquivos .zip do portal da Receita para uma pasta temporária.
    * Descompactação: Extrai o conteúdo dos arquivos .zip (arquivos .csv, .socios, .estabele, etc.).

- Etapa 2: Transformação
    * Correção de Formato: Detecta o encoding original de cada arquivo e o converte para um padrão único e moderno (UTF-8).
    * Renomeação: Salva os arquivos corrigidos com um sufixo (ex: __corrigido) em uma pasta de destino final, mantendo os arquivos originais intactos para referência.

- Etapa 3: Validação Pós-Transformação
    * Contagem de Linhas: Compara a quantidade de linhas do arquivo original com a do arquivo corrigido para garantir que nenhum dado foi perdido durante a conversão. O processo é abortado se houver divergência.
    
- Etapa 4: Carga
    * Preparação do Banco de Dados: Conecta-se ao PostgreSQL e cria um schema específico para o mês e ano dos dados (ex: rfb_202509).
    * Carga de Dados: Utiliza o comando COPY do PostgreSQL, a forma mais eficiente de carregar os milhões de registros dos arquivos corrigidos para as tabelas correspondentes.

- Etapa 5: Validação Pós-Carga
    * Verificação de Integridade: Após a carga, executa uma série de testes SQL automáticos na base de dados para confirmar: 
        * A contagem de registros na tabela bate com a do arquivo.
        * Não há deslocamento de colunas (checando valores nulos em colunas essenciais e formatos de dados).
    * Em caso de falha, a operação pode ser revertida (ROLLBACK) para não manter dados inconsistentes no banco.

- Notificação e Finalização
    * Sucesso: Ao final do processo, envia uma notificação (ex: por e-mail) informando que a carga foi concluída com sucesso e os dados estão prontos para uso.
    * Falha: Se ocorrer qualquer erro em qualquer etapa, a pipeline para e envia um alerta detalhado, informando exatamente onde e por que a falha ocorreu.