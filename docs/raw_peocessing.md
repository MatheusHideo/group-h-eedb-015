# Documentação do Processo de Ingestão de Dados de Seguros de Saúde 

## Visão Geral

Este script Python é projetado para processar dados na camada RAW. Os dados são referente ao mercado de seguros de saúde dos anos 2014, 2015 e 2016, armazenados em um arquivo ZIP no Amazon S3. Ele extrai os dados, realiza transformações necessárias e salva os resultados de volta no S3 em formato Parquet, otimizado para consultas e processamento da camada CLEANESED.

dataset: hhs/health-insurance-marketplace (kaggle) 

## Dependências

- Python 3.7+
- pandas
- numpy
- boto3
- pyarrow

## Configuração

Antes de executar o script, certifique-se de configurar as seguintes variáveis:

- `INPUT_BUCKET`: Nome do bucket S3 onde o arquivo ZIP de entrada está armazenado.
- `OUTPUT_BUCKET`: Nome do bucket S3 onde os arquivos Parquet processados serão salvos.
- `ZIP_FILE_KEY`: Caminho do arquivo ZIP dentro do bucket de entrada.

Além disso, configure suas credenciais AWS adequadamente para permitir acesso aos buckets S3.

## Estrutura do Código

### Importações e Configurações Iniciais

O script começa importando as bibliotecas necessárias e configurando o logger e o cliente S3.

### Funções Auxiliares

1. `normalize_file_name(file_name: str) -> str`:
   Normaliza o nome do arquivo removendo sufixos específicos.

2. `ensure_s3_directory(bucket: str, path: str) -> None`:
   Garante que um diretório (prefixo) existe no S3.

3. `read_csv_from_s3(bucket: str, key: str, chunksize: Optional[int] = None) -> Union[pd.DataFrame, pd.io.parsers.TextFileReader]`:
   Lê um arquivo CSV diretamente do S3, com suporte para leitura em chunks.

4. `validate_data(df: pd.DataFrame) -> Dict`:
   Realiza validações básicas nos dados e retorna estatísticas.

5. `is_csv_file(filename: str) -> bool`:
   Verifica se um arquivo é CSV com base na extensão.

6. `generate_version_hash(df: pd.DataFrame) -> str`:
   Gera um hash MD5 para versionar os dados.

### Função Principal de Processamento

`process_and_save_file(bucket: str, key: str, table_name: str) -> None`:
Esta função é o coração do script. Ela:
- Lê os dados do S3 em chunks
- Processa cada chunk, adicionando metadados
- Compara com dados existentes para identificar novos registros ou atualizações
- Valida os dados
- Salva os resultados de volta no S3 em formato Parquet

### Processamento do Arquivo ZIP

`process_zip_file()`:
Esta função:
- Recupera o arquivo ZIP do S3
- Extrai seu conteúdo (implementação pendente)
- Processa cada arquivo CSV extraído

## Fluxo de Execução

1. O script é iniciado, configurando o logger e o cliente S3.
2. A função `process_zip_file()` é chamada, que:
   - Recupera o arquivo ZIP do S3
   - Extrai os arquivos CSV
   - Para cada arquivo CSV:
     - Normaliza o nome da tabela
     - Chama `process_and_save_file()` para processar o arquivo
3. `process_and_save_file()` processa cada arquivo:
   - Lê os dados em chunks
   - Adiciona metadados (ingestDate, partitionDate, version)
   - Compara com dados existentes
   - Realiza validações
   - Salva os resultados processados no S3 em formato Parquet

## Logging

O script utiliza o módulo `logging` do Python para registrar informações importantes, avisos e erros durante a execução. Os logs incluem:
- Início e conclusão do processamento de arquivos
- Estatísticas de validação de dados
- Erros encontrados durante o processamento

## Considerações de Uso

- Certifique-se de ter permissões adequadas no AWS IAM para ler e escrever nos buckets S3 especificados.
- Ajuste o tamanho do chunk (`chunk_size`) conforme necessário, dependendo da memória disponível e do tamanho dos arquivos.
- A implementação da extração do arquivo ZIP precisa ser finalizada na função `process_zip_file()`.
- Considere adicionar mais validações de dados conforme necessário para seu caso de uso específico.

## Execução

Para executar o script: 
```python
> python raw_processing_aws.py
```

## Developers

1. **Turma:** eEDB-015/2025-1 - Projeto Integrador

2. **Grupo:** H
    - Marcelo Dozzi Barbugli
    - Gisele Siqueira
    - Roberto Eyama
    - Matheus Higa
    - Ricardo Geroto

3. **Versão:** V1.1 (22/03/2025)