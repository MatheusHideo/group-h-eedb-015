# -*- coding: utf-8 -*-
"""
Data Transform: tb_silver_service_area

Este módulo contém funções para processar e transformar dados de áreas de serviço,
unindo-os com dados de CEP e salvando o resultado em formato Parquet no S3.

Principais funcionalidades:
- Leitura e processamento de dados em chunks
- Join com tabela de CEPs
- Salvamento de dados em formato Parquet
- Logging de métricas e informações de processo
"""

import io
import logging
import warnings
import hashlib
import time
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from typing import List, Optional

warnings.filterwarnings('ignore')

# Configuração do logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configurações
APP_NAME = "tb_silver_service_area"
TABLE_NAME = "tb_silver_service_area"
S3_BUCKET = "raw-test-edb"
S3_OUTPUT_BUCKET = "cleaned-test-edb"
FILE_PATH_1 = f"Service_Area/"
FILE_PATH_2 = f"ServiceArea/"
ZIPCODE_PATH = f"tb_bronze_zipcodes/"
OUTPUT_PATH = f"{APP_NAME}/{TABLE_NAME}/"

COLUMNS: List[str] = ["BusinessYear", "IssuerId", "StateCode", "ServiceAreaId", "ServiceAreaName", "MarketCoverage", "VersionNum", "County", "CoverEntireState", "version"]

CHUNK_SIZE = 200000
BATCH_SIZE = 40

# Inicializar cliente S3
s3 = boto3.client('s3')

# Decorador para log de tempo de execução
def log_execution_time(func):
    """
    Decorador para logar o tempo de execução de uma função.

    Args:
        func (callable): A função a ser decorada.

    Returns:
        callable: A função decorada.
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"{func.__name__} executado em {end_time - start_time:.2f} segundos")
        return result
    return wrapper

def generate_version(row: pd.Series, update_type: str = 'insert') -> str:
    """
    Gera uma string de versão única para uma linha de dados.

    Args:
        row (pd.Series): A linha de dados.
        update_type (str, optional): Tipo de atualização. Padrão é 'insert'.

    Returns:
        str: Uma string de versão única.
    """
    data = ''.join(str(val) for val in row.values) + str(pd.Timestamp.now())
    version_string = f"{update_type}_{hashlib.md5(data.encode()).hexdigest()}"
    return version_string

@log_execution_time
def process_chunk(chunk: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    """
    Processa um chunk de dados, adicionando novas linhas ou atualizando existentes.

    Args:
        chunk (pd.DataFrame): O chunk de dados a ser processado.
        existing_df (pd.DataFrame): O DataFrame existente para comparação.

    Returns:
        pd.DataFrame: O DataFrame resultante após o processamento.
    """
    if not isinstance(chunk, pd.DataFrame):
        logger.error(f"Chunk não é um DataFrame. Tipo recebido: {type(chunk)}")
        return existing_df

    chunk['ingestDate'] = pd.Timestamp.now()
    columns_to_compare = [c for c in COLUMNS if c not in ['ingestDate', 'partitionDate', 'version']]

    if existing_df.empty:
        chunk['version'] = chunk.apply(generate_version, axis=1)
        return chunk

    new_or_updated = chunk[~chunk.set_index(columns_to_compare).index.isin(existing_df.set_index(columns_to_compare).index)]
    if not new_or_updated.empty:
        new_or_updated['version'] = new_or_updated.apply(generate_version, axis=1)
        return pd.concat([existing_df, new_or_updated]).drop_duplicates(subset=columns_to_compare, keep='last')
    return existing_df


def get_latest_partition(prefix, bucket):
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
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
        partitions = [obj['Prefix'] for obj in response.get('CommonPrefixes', []) if obj['Prefix'].startswith(f"{prefix}partition_date=") or obj['Prefix'].startswith(f"{prefix}partitionDate=") ]
        return max(partitions).split('=')[1].rstrip('/')
    except Exception as e:
        logger.error(f"Erro ao obter a última partição: {str(e)}")
        raise

def process_file(file_path, columns, chunk_size):
    """
    Processa um arquivo Parquet do S3, dividindo-o em chunks.

    Args:
        file_path (str): Caminho do arquivo no S3.
        columns (List[str]): Lista de colunas a serem incluídas.
        chunk_size (int): Tamanho de cada chunk.

    Returns:
        List[pd.DataFrame]: Lista de chunks do DataFrame.
    """
    last_partition = get_latest_partition(file_path, S3_BUCKET)
    key_path = f"{file_path}partition_date={last_partition}/{file_path.replace('/', '')}_1.parquet"
    s3_obj = s3.get_object(Bucket=S3_BUCKET, Key=key_path)
    table = pq.read_table(io.BytesIO(s3_obj['Body'].read()))

    if 'partitionDate' in table.column_names:
        partitionDate_index = table.column_names.index('partitionDate')
        table = table.set_column(
            partitionDate_index,
            'partitionDate',
            table.column('partitionDate').cast(pa.string())
        )
    if 'County' in table.column_names:
        county_index = table.column_names.index('County')
        table = table.set_column(
            county_index,
            'County',
            table.column('County').cast(pa.int64())
        )

    num_rows = table.num_rows
    chunks = []
    for i in range(0, num_rows, chunk_size):
        chunk = table.slice(i, chunk_size).to_pandas()
        chunks.append(chunk)
    return chunks

@log_execution_time
def read_and_process_data_in_chunks(file_path_1: str, file_path_2: str, columns: List[str]) -> pd.DataFrame:
    """
    Lê e processa dados de dois arquivos em chunks.

    Args:
        file_path_1 (str): Caminho do primeiro arquivo no S3.
        file_path_2 (str): Caminho do segundo arquivo no S3.
        columns (List[str]): Lista de colunas a serem incluídas.

    Returns:
        pd.DataFrame: DataFrame resultante após o processamento de todos os chunks.
    """
    logger.info(f"Lendo e processando dados de {file_path_1} e {file_path_2}")

    all_chunks = []
    for file_path in [file_path_1, file_path_2]:
        logger.info(f"Processando arquivo: {file_path}")
        chunks = process_file(file_path, columns, CHUNK_SIZE)
        logger.info(f"Número de chunks do arquivo {file_path}: {len(chunks)}")
        all_chunks.extend(chunks)

    logger.info(f"Total de chunks a serem processados: {len(all_chunks)}")

    result_df = pd.DataFrame(columns=columns)
    for i, chunk in enumerate(all_chunks):
        logger.info(f"Processando chunk {i+1}/{len(all_chunks)}")
        result_df = process_chunk(chunk, result_df)

    logger.info(f"Número total de linhas após processamento: {len(result_df)}")
    result_df['partitionDate'] = pd.Timestamp.now().strftime("%Y%m%d")
    return result_df

@log_execution_time
def join_with_zipcodes(df: pd.DataFrame, zipcode_path: str) -> pd.DataFrame:
    """
    Realiza um join entre o DataFrame principal e a tabela de CEPs.

    Args:
        df (pd.DataFrame): DataFrame principal.
        zipcode_path (str): Caminho da tabela de CEPs no S3.

    Returns:
        pd.DataFrame: DataFrame resultante após o join.
    """
    logger.info("Realizando join com tabela tb_silver_zipcodes")
    last_partition = get_latest_partition(zipcode_path, S3_OUTPUT_BUCKET)
    key_path = f"{zipcode_path}partitionDate={last_partition}/data_{last_partition}.parquet"
    s3_obj = s3.get_object(Bucket=S3_OUTPUT_BUCKET, Key=key_path)
    zipcode_df = pd.read_parquet(io.BytesIO(s3_obj['Body'].read()))

    if 'partitionDate' in df.columns:
        df['partitionDate'] = df['partitionDate'].astype(str)
    if 'County' in df.columns:
        df['County'] = df['County'].astype(str)

    joined_df = pd.merge(df, zipcode_df, on="ServiceAreaId", how="inner")

    columns_to_drop = ["ingestDate_y", "partitionDate_y", "ingestDate_x", "partitionDate_x"]
    joined_df = joined_df.drop(columns=[col for col in columns_to_drop if col in joined_df.columns])

    logger.info(f"Número de registros após o join: {len(joined_df)}")
    logger.info(f"Colunas no DataFrame final: {joined_df.columns}")
    logger.info(f"Tipos de dados das colunas: {joined_df.dtypes}")

    null_counts = joined_df.isnull().sum()
    if null_counts.any():
        logger.warning(f"Valores nulos encontrados:\n{null_counts[null_counts > 0]}")

    return joined_df

@log_execution_time
def save_as_parquet(df: pd.DataFrame, output_path: str, columns_to_compare: Optional[List[str]] = None):
    """
    Salva o DataFrame como arquivo Parquet no S3.

    Args:
        df (pd.DataFrame): DataFrame a ser salvo.
        output_path (str): Caminho de saída no S3.
        columns_to_compare (Optional[List[str]]): Colunas a serem usadas para comparação.

    Returns:
        None
    """
    logger.info(f"Iniciando salvamento de dados como Parquet em {output_path}")

    if columns_to_compare is None:
        columns_to_compare = [col for col in df.columns if col not in ['ingestDate', 'partitionDate', 'version']]

    current_partition = datetime.now().strftime("%Y%m%d")

    try:
        s3_obj = s3.get_object(Bucket=S3_OUTPUT_BUCKET, Key=output_path)
        existing_df = pd.read_parquet(io.BytesIO(s3_obj['Body'].read()))
    except Exception as e:
        logger.warning(f"Erro ao ler partições existentes: {e}. Assumindo que não existem partições.")
        existing_df = pd.DataFrame(columns=df.columns)

    df['partitionDate'] = current_partition

    final_df = pd.DataFrame(columns=df.columns)
    for chunk in [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]:
        final_df = process_chunk(chunk, final_df)

    try:
        buffer = io.BytesIO()
        final_df.to_parquet(buffer, index=False)
        s3.put_object(Bucket=S3_OUTPUT_BUCKET, Key=f"{output_path}data_{current_partition}.parquet", Body=buffer.getvalue())
        logger.info("Dados salvos com sucesso")
    except Exception as e:
        logger.error(f"Erro ao salvar dados: {e}")
        return

    # Verificações e métricas
    s3_obj = s3.get_object(Bucket=S3_OUTPUT_BUCKET, Key=f"{output_path}data_{current_partition}.parquet")
    saved_df = pd.read_parquet(io.BytesIO(s3_obj['Body'].read()))
    logger.info("Schema dos dados salvos:")
    logger.info(saved_df.dtypes)
    logger.info("Primeiras 5 linhas dos dados salvos:")
    logger.info(saved_df.head())

    total_count = len(saved_df)
    logger.info(f"Contagem total de registros salvos: {total_count}")

    current_partition_df = saved_df[saved_df['partitionDate'] == current_partition]
    current_partition_count = len(current_partition_df)
    logger.info(f"Registros na partição atual ({current_partition}): {current_partition_count}")

    duplicates = saved_df.groupby(COLUMNS).size().reset_index(name='count')
    duplicates = duplicates[duplicates['count'] > 1]
    duplicate_count = len(duplicates)
    logger.info(f"Número de registros duplicados globalmente: {duplicate_count}")
    if duplicate_count > 0:
        logger.info("Exemplo de duplicatas:")
        logger.info(duplicates.head())

@log_execution_time
def lambda_handler(event, context):
    """
    Função principal do Lambda que orquestra todo o processo de transformação de dados.

    Args:
        event (dict): Evento que acionou o Lambda.
        context (object): Contexto de execução do Lambda.

    Returns:
        dict: Resposta da execução do Lambda.
    """
    try:
        logger.info("Iniciando processo")

        df_selected = read_and_process_data_in_chunks(FILE_PATH_1, FILE_PATH_2, COLUMNS)
        df_joined = join_with_zipcodes(df_selected, ZIPCODE_PATH)
        columns_to_compare = [col for col in df_joined.columns if col not in ['ingestDate', 'partitionDate', 'version']]
        save_as_parquet(df_joined, OUTPUT_PATH, columns_to_compare)
        logger.info("Processo concluído com sucesso")

        return {
            'statusCode': 200,
            'body': 'Processo executado com sucesso'
        }

    except Exception as e:
        logger.error(f"Erro durante a execução: {str(e)}")
        import traceback
        logger.error(f"Traceback completo:\n{traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': f'Erro durante a execução: {str(e)}'
        }
        
