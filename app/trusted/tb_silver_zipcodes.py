"""
Este script processa dados de 'raw/tb_service_area' e 'raw/tb_servicearea', para criar a tabela 'silver/tb_silver_zip_codes' 
armazenando-os no Amazon S3.

O script lê dados de áreas de serviço de duas fontes diferentes no S3, processa-os para extrair
códigos postais, e então salva os resultados processados de volta no S3 em formato Parquet.

Requer:
- Acesso configurado ao Amazon S3
- Bibliotecas: boto3, pandas

Uso:
- Como função Lambda: Configurar o handler como 'script_name.lambda_handler'
- Executar localmente: python script_name.py
"""

import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import logging
import json

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Configurações
APP_NAMES = "tb_bronze_zipcodes"
TABLE_NAME = "tb_bronze_zipcodes"
BUCKET_NAME = "raw-test-edb"
OUTPUT_BUCKET_NAME = "cleaned-test-edb"
BRONZE_PREFIX = f""
SILVER_PREFIX = f"{TABLE_NAME}/"

# Inicializar cliente S3
s3 = boto3.client('s3')

# Incremental e Gerenciamento de Partições
def get_latest_partition(prefix):
    """
    Obtém a partição mais recente de um prefixo específico no S3.

    Args:
    prefix (str): O prefixo do caminho no S3 para buscar partições.

    Returns:
    str: A data da partição mais recente.

    Raises:
    Exception: Se houver um erro ao listar objetos no S3.
    """
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
        partitions = [obj['Prefix'] for obj in response.get('CommonPrefixes', []) if obj['Prefix'].startswith(f"{prefix}partition_date=")]
        return max(partitions).split('=')[1].rstrip('/')
    except Exception as e:
        logger.error(f"Erro ao obter a última partição: {str(e)}")
        raise

# Funções de Leitura e Escrita especcíficas para a tabela 'tb_silver_zipcodes'
def read_parquet_from_s3(path):
    """
    Lê um arquivo Parquet do S3 e retorna como DataFrame.

    Args:
    path (str): O caminho completo do arquivo Parquet no S3.

    Returns:
    pandas.DataFrame: O DataFrame lido do arquivo Parquet.

    Raises:
    Exception: Se houver um erro ao ler o arquivo do S3.
    """
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=path)
        return pd.read_parquet(BytesIO(obj['Body'].read()))
    except Exception as e:
        logger.error(f"Erro ao ler arquivo Parquet do S3: {str(e)}")
        raise

def process_df(df):
    """
    Processa o DataFrame, explodindo a coluna ZipCodes e adicionando timestamps.

    Args:
    df (pandas.DataFrame): O DataFrame a ser processado.

    Returns:
    pandas.DataFrame: O DataFrame processado.

    Raises:
    Exception: Se houver um erro durante o processamento.
    """
    try:
        df_exploded = df.assign(ZipCode=df['ZipCodes'].str.split(',')).explode('ZipCode')
        df_exploded = df_exploded[['ServiceAreaId', 'ZipCode']]
        df_exploded['ingestDate'] = datetime.now()
        df_exploded['partitionDate'] = datetime.now().strftime('%Y%m%d')
        return df_exploded
    except Exception as e:
        logger.error(f"Erro ao processar DataFrame: {str(e)}")
        raise

def save_parquet_to_s3(df, path):
    """
    Salva um DataFrame como arquivo Parquet no S3.

    Args:
    df (pandas.DataFrame): O DataFrame a ser salvo.
    path (str): O caminho completo no S3 onde o arquivo será salvo.

    Raises:
    Exception: Se houver um erro ao salvar o arquivo no S3.
    """
    try:
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        s3.put_object(Bucket=OUTPUT_BUCKET_NAME, Key=path, Body=buffer.getvalue())
    except Exception as e:
        logger.error(f"Erro ao salvar arquivo Parquet no S3: {str(e)}")
        raise

def main():
    """
    Função principal que orquestra o processamento dos dados.

    Esta função realiza as seguintes etapas:
    1. Obtém as partições mais recentes dos dados de origem.
    2. Lê os dados do S3.
    3. Processa os DataFrames.
    4. Une os DataFrames processados.
    5. Agrupa e seleciona os dados mais recentes.
    6. Salva os resultados de volta no S3.

    Raises:
    Exception: Se houver um erro durante qualquer etapa do processamento.
    """
    try:
        # Obter as partitionDates mais recentes
        latest_partition1 = get_latest_partition(f"{BRONZE_PREFIX}Service_Area/")
        latest_partition2 = get_latest_partition(f"{BRONZE_PREFIX}ServiceArea/")

        # Ler apenas as partições mais recentes
        df1 = read_parquet_from_s3(f"{BRONZE_PREFIX}Service_Area/partition_date={latest_partition1}/Service_Area_1.parquet")
        df2 = read_parquet_from_s3(f"{BRONZE_PREFIX}ServiceArea/partition_date={latest_partition2}/ServiceArea_1.parquet")

        # Processar ambos os DataFrames
        df1_exploded = process_df(df1)
        df2_exploded = process_df(df2)

        # Realizar o union dos dois DataFrames processados
        united_df = pd.concat([df1_exploded, df2_exploded], ignore_index=True)

        # Agrupar por todas as colunas exceto ingestDate e partitionDate, e selecionar o partitionDate mais recente
        columns_to_group = [c for c in united_df.columns if c not in ['ingestDate', 'partitionDate']]
        latest_df = united_df.sort_values('partitionDate', ascending=False).drop_duplicates(subset=columns_to_group)

        # Iterar sobre cada data de partição única
        for partition_date in latest_df['partitionDate'].unique():
            df_partition = latest_df[latest_df['partitionDate'] == partition_date]
            partition_path = f"{SILVER_PREFIX}partitionDate={partition_date}/data_{partition_date}.parquet"
            save_parquet_to_s3(df_partition, partition_path)

        logger.info(f"Processamento concluído. Número total de linhas mais recentes: {len(latest_df)}")
        logger.info(f"PartitionDates únicas: {latest_df['partitionDate'].unique()}")

    except Exception as e:
        logger.error(f"Erro durante a execução: {str(e)}")
        raise

def lambda_handler(event, context):
    """
    Handler da função Lambda.

    Args:
    event (dict): O evento que acionou a função Lambda.
    context (object): O contexto de execução da função Lambda.

    Returns:
    dict: Um dicionário contendo o status da execução e informações adicionais.
    """
    try:
        main()
        return {
            'statusCode': 200,
            'body': json.dumps('Processamento concluído com sucesso.')
        }
    except Exception as e:
        logger.error(f"Erro durante a execução da função Lambda: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Erro durante o processamento: {str(e)}')
        }

# if __name__ == "__main__":
#     main()