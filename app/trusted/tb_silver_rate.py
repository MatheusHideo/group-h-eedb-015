import re
import pandas as pd
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import hashlib
import logging
import boto3
import traceback
import io

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constantes
APP_NAME = "Rate"
TABLE_NAME = "tb_silver_rate"

# Configuração AWS
s3_client = boto3.client('s3')
S3_BUCKET = 'raw-test-edb'  # Substitua pelo nome do seu bucket S3
S3_OUTPUT_BUCKET = 'cleaned-test-edb'
INPUT_PREFIX = f'{APP_NAME}'
OUTPUT_PREFIX = f'{TABLE_NAME}' ## mudar

# Lista de colunas a serem processadas
COLUMNS = [
    'BusinessYear',
    'StateCode',
    'IssuerId',
    'SourceName',
    'VersionNum',
    'ImportDate',
    'IssuerId2',
    'FederalTIN',
    'RateEffectiveDate',
    'RateExpirationDate',
    'PlanId',
    'RatingAreaId',
    'Tobacco',
    'Age',
    'IndividualRate',
    'IndividualTobaccoRate',
    'Couple',
    'PrimarySubscriberAndOneDependent',
    'PrimarySubscriberAndTwoDependents',
    'PrimarySubscriberAndThreeOrMoreDependents',
    'CoupleAndOneDependent',
    'CoupleAndTwoDependents',
    'CoupleAndThreeOrMoreDependents',
    'RowNumber'
]

def create_table_structure():
    """
    Cria a estrutura da tabela com tipos de dados específicos para cada coluna.
    """
    logger.info("Criando estrutura da tabela")
    
    # Definindo os tipos de dados para cada coluna
    
    fields = [
            ('BusinessYear', pa.int8()),
            ('StateCode', pa.string()),
            ('IssuerId', pa.int8()),
            ('SourceName', pa.string()),
            ('VersionNum', pa.int8()),
            ('ImportDate', pa.string()),
            ('IssuerId2', pa.int8()),
            ('FederalTIN', pa.string()),
            ('RateEffectiveDate', pa.string()),
            ('RateExpirationDate', pa.string()),
            ('PlanId', pa.string()),
            ('RatingAreaId', pa.string()),
            ('Tobacco', pa.string()),
            ('Age', pa.string()),
            ('IndividualRate', pa.decimal128(precision=10, scale=2)),
            ('IndividualTobaccoRate', pa.decimal128(precision=10, scale=2)),
            ('Couple', pa.string()),
            ('PrimarySubscriberAndOneDependent', pa.string()),
            ('PrimarySubscriberAndTwoDependents', pa.string()),
            ('PrimarySubscriberAndThreeOrMoreDependents', pa.string()),
            ('CoupleAndOneDependent', pa.string()),
            ('CoupleAndTwoDependents', pa.string()),
            ('CoupleAndThreeOrMoreDependents', pa.string()),
            ('RowNumber', pa.int8()),
            ('partitionDate', pa.date32()),
            ('ingestDate', pa.timestamp('us')),
            ('version', pa.string())
    ]
    
    schema = pa.schema(fields)
    
    empty_data = {field.name: pa.array([], type=field.type) for field in schema} 
       
    empty_table = pa.Table.from_pydict(empty_data, schema=schema)
    
    return empty_table

def generate_version(row, update_type='insert'):
    """
    Gera uma string de versão única para cada linha de dados.
    """
    data = ''.join(str(val) for val in row.values) + str(pd.Timestamp.now())
    version_string = f"{update_type}_{hashlib.md5(data.encode()).hexdigest()}"
    return version_string

def process_file(s3_key):
    """
    Processa um único arquivo Parquet do S3.
    """
    logger.info(f"Processando arquivo: {s3_key}")
    try:
        # Extrai a data de partição do nome do arquivo
        partition_date = re.search(r'partitionDate=(\d{8})', s3_key)
        partition_date = partition_date.group(1) if partition_date else datetime.now().strftime("%Y%m%d")

        # Lê o arquivo Parquet do S3
        with s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)['Body'] as obj:
            df = pd.read_parquet(io.BytesIO(obj.read()))

        # Seleciona apenas as colunas especificadas
        if COLUMNS:
            df = df[COLUMNS]
        
        # Adiciona colunas adicionais
        df['partitionDate'] = partition_date
        df['ingestDate'] = pd.Timestamp.now()
        df['version'] = df.apply(generate_version, axis=1)

        return df
    except Exception as e:
        logger.error(f"Erro ao processar arquivo {s3_key}: {str(e)}")
        return pd.DataFrame()

def read_and_process_data():
    """
    Lê e processa todos os arquivos Parquet do S3.
    """
    logger.info(f"Lendo e processando dados do S3")

    try:
        # Lista todos os arquivos Parquet no bucket S3
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=INPUT_PREFIX)

        all_files = [obj['Key'] for page in pages for obj in page['Contents'] if obj['Key'].endswith('.parquet')]

        logger.info(f"Total de arquivos Parquet encontrados: {len(all_files)}")

        if not all_files:
            logger.info("Nenhum arquivo Parquet encontrado.")
            return pd.DataFrame()

        # Processa cada arquivo e concatena os resultados
        dfs = []
        for file in all_files:
            df = process_file(file)
            if not df.empty:
                dfs.append(df)

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        else:
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Erro ao ler e processar dados: {str(e)}")
        logger.error(f"Traceback completo:\n{traceback.format_exc()}")
        raise

def save_as_parquet(df: pd.DataFrame):
    """
    Salva o DataFrame processado como um arquivo Parquet no S3.
    """
    if df.empty:
        logger.info("DataFrame está vazio. Nada para salvar.")
        return

    logger.info(f"Salvando dados como Parquet no S3")

    try:
        # Converte todas as colunas para string
        for col in df.columns:
            df[col] = df[col].astype(str)

        # Converte o DataFrame para uma tabela PyArrow
        table = pa.Table.from_pandas(df)
        
        # Escreve a tabela em um buffer
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer)

        # Gera um nome de arquivo único
        s3_key = f"{OUTPUT_PREFIX}/data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        
        # Salva o buffer no S3
        s3_client.put_object(Bucket=S3_OUTPUT_BUCKET, Key=s3_key, Body=buffer.getvalue().to_pybytes())

        logger.info(f"Dados salvos com sucesso no S3: {s3_key}")

    except Exception as e:
        logger.error(f"Erro ao salvar dados: {str(e)}")
        raise

def main():
    """
    Função principal que orquestra todo o processo.
    """
    try:
        logger.info("Iniciando processo")
        logger.info(f"COLUMNS: {COLUMNS}")

        # Criar a estrutura da tabela
        empty_table = create_table_structure()
        logger.info(f"Estrutura da tabela criada: {empty_table.schema}")

        # Ler e processar os dados
        df_processed = read_and_process_data()
        if df_processed.empty:
            logger.info("Nenhum dado encontrado nos arquivos de entrada.")
            return

        # Converter o DataFrame processado para uma tabela PyArrow
        processed_table = pa.Table.from_pandas(df_processed)

        # Verificar se o esquema do DataFrame processado corresponde ao esquema da tabela vazia
        if processed_table.schema != empty_table.schema:
            logger.warning("O esquema dos dados processados não corresponde à estrutura da tabela.")
            # Aqui você pode adicionar lógica adicional para lidar com diferenças de esquema

        # Salvar os dados processados
        save_as_parquet(df_processed)

        logger.info("Processo concluído com sucesso")

    except Exception as e:
        logger.error(f"Erro durante a execução: {str(e)}")
        logger.error(f"Traceback completo:\n{traceback.format_exc()}")

def lambda_handler(event, context):
    """
    Função handler para AWS Lambda.
    """
    try:
        main()
        return {
            "statusCode": 200,
            "body": "Processamento concluído com sucesso"
        }
    except Exception as e:
        logger.error(f"Erro durante a execução do script: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "statusCode": 500,
            "body": f"Erro durante o processamento: {str(e)}"
        }
