import os
import re
import csv
import hashlib
import traceback
import gc
from typing import Dict, Optional, Union
import pandas as pd
import numpy as np
import boto3
from botocore.exceptions import ClientError
import io
import zipfile
import logging
import json

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuração AWS
s3_client = boto3.client('s3')
input_bucket = 'landing-test-edb'
output_bucket = 'raw-test-edb'
zip_key = 'health-insurance-marketplace'

# Anos para processar
years_to_process = ['2014', '2015', '2016']

def download_and_extract_zip(bucket: str, key: str) -> str:
    """
    Baixa o arquivo zip do S3 e extrai seu conteúdo para um diretório temporário.
    """
    try:
        zip_obj = s3_client.get_object(Bucket=bucket, Key=key)
        buffer = io.BytesIO(zip_obj['Body'].read())
        
        with zipfile.ZipFile(buffer) as zip_file:
            temp_dir = '/tmp/extracted_data'
            zip_file.extractall(temp_dir)
        
        logger.info(f"Arquivo ZIP extraído com sucesso para {temp_dir}")
        return temp_dir
    except ClientError as e:
        logger.error(f"Erro ao baixar ou extrair o arquivo ZIP: {e}")
        raise

def normalize_file_name(file_name: str) -> str:
    return re.sub(r"_PUF.*", "", file_name)

def read_csv_with_options(file_path: str, chunksize: Optional[int] = None) -> Union[pd.DataFrame, pd.io.parsers.TextFileReader]:
    options = [
        {"header": 0},
        {"header": 0, "sep": ","},
        {"header": 0, "sep": "\t"},
        {"header": 0, "encoding": "ISO-8859-1"}
    ]

    for option in options:
        try:
            return pd.read_csv(file_path, **option, low_memory=False, chunksize=chunksize)
        except Exception as e:
            logger.warning(f"Falha ao ler {file_path} com opções {option}: {str(e)}")

    logger.info(f"Tentando inferir schema manualmente para {file_path}")
    with open(file_path, 'r') as file:
        header = next(csv.reader(file))
    return pd.read_csv(file_path, header=0, names=header, chunksize=chunksize)

def validate_data(df: pd.DataFrame) -> Dict:
    total_rows = len(df)
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    other_columns = df.columns.difference(numeric_columns)
    numeric_null_counts = df[numeric_columns].isnull().sum() if not numeric_columns.empty else pd.Series(dtype=float)
    other_null_counts = df[other_columns].isnull().sum() if not other_columns.empty else pd.Series(dtype=float)
    numeric_null_counts = numeric_null_counts[numeric_null_counts > 0]
    other_null_counts = other_null_counts[other_null_counts > 0]
    null_counts = pd.concat([numeric_null_counts, other_null_counts])

    validation_results = {
        "total_rows": total_rows,
        "columns": df.columns.tolist(),
        "null_percentages": (null_counts / total_rows * 100).to_dict()
    }

    return validation_results

def is_csv_file(filename: str) -> bool:
    return filename.lower().endswith('.csv')

def generate_version_hash(df: pd.DataFrame) -> str:
    return hashlib.md5(pd.util.hash_pandas_object(df).values).hexdigest()

def save_to_s3(df: pd.DataFrame, bucket: str, key: str):
    """
    Salva o DataFrame como um arquivo Parquet no S3.
    """
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    logger.info(f"Arquivo salvo no S3: s3://{bucket}/{key}")

def process_and_save_file(file_path: str, table_name: str) -> None:
    try:
        logger.info(f"Começando o processamento do arquivo: {file_path}")
        if os.path.basename(file_path).startswith('.') or not is_csv_file(file_path):
            logger.info(f"Ignorando arquivo não CSV ou oculto: {file_path}")
            return

        if not os.path.exists(file_path):
            logger.info(f"Arquivo não encontrado: {file_path}")
            return

        table_name = normalize_file_name(os.path.basename(file_path))
        chunk_size = 100000
        chunks = read_csv_with_options(file_path, chunksize=chunk_size)
    
        compare_columns = None
        new_records_total = 0
        processed_chunks = 0

        for chunk in chunks:
            processed_chunks += 1
            logger.info(f"Processando chunk {processed_chunks} para {table_name}")

            if chunk.empty:
                continue

            chunk = chunk.infer_objects()
            chunk['ingestDate'] = pd.Timestamp.now()
            chunk['partitionDate'] = pd.Timestamp.now().strftime("%Y%m%d")
            chunk['version'] = generate_version_hash(chunk)

            if compare_columns is None:
                compare_columns = [col for col in chunk.columns if col not in ["ingestDate", "partitionDate", "version"]]

            # Aqui você pode implementar a lógica para verificar dados existentes no S3, se necessário

            validation_results = validate_data(chunk)
            logger.info(f"Validação para {table_name}: {validation_results}")

            # Salvar no S3
            s3_key = f"{table_name}/partition_date={chunk['partitionDate'].iloc[0]}/{table_name}_{processed_chunks}.parquet"
            save_to_s3(chunk, output_bucket, s3_key)

            new_records_total += len(chunk)

            del chunk
            gc.collect()

        logger.info(f"Processamento concluído para {file_path}")
        logger.info(f"Total de novos registros para {table_name}: {new_records_total}")

    except Exception as e:
        logger.error(f"Erro ao processar {file_path}: {str(e)}")
        logger.error(traceback.format_exc())

def process_directory(base_path: str) -> None:
    for root, _, files in os.walk(base_path):
        year = os.path.basename(root)
        if year in years_to_process:
            for file in files:
                if is_csv_file(file):
                    file_path = os.path.join(root, file)
                    normalized_name = normalize_file_name(file)
                    table_name = f"tb_{normalized_name.lower()}"
                    process_and_save_file(file_path, table_name)


def handler(event, context):
    try:
        extracted_path = download_and_extract_zip(input_bucket, zip_key)
        process_directory(extracted_path)
        return {
           "statusCode": 200,
            "body": "OK"
    }
    except Exception as e:
        logger.error(f"Erro durante a execução do script: {str(e)}")
        logger.error(traceback.format_exc())
        return {
         "statusCode": 400,
         "body": "error"
        }
        
