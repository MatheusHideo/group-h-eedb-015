import os
import re
import boto3
import pandas as pd
import pandera as pa
from pandera import Column
import logging
import tempfile

# ========== AWS CLIENT ==========
s3 = boto3.client('s3')

# ========== LOGGING CONFIG ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ========== CONFIGURAÇÕES ==========
BUCKET_SILVER = "cleaned-test-edb"
PREFIX_SILVER = "tb_silver"
BUCKET_GOLD = "delivery-test-edb"
PREFIX_GOLD = "tb_gold"

# Schema Pandera
schema = pa.DataFrameSchema({
    "BusinessYear": Column(pa.Int, nullable=False, coerce=True),
    "PlanId": Column(pa.String, nullable=True, required=False),
    "IssuerId": Column(pa.Int, nullable=False, coerce=True),
    "StateCode": Column(pa.String, nullable=False, required=False),
    "SourceName": Column(pa.String, nullable=False, required=False),
})

# ========== FUNÇÕES ==========
def extrair_particoes(caminho):
    padrao = re.compile(r"([a-zA-Z0-9_]+)=([^\/]+)")
    return dict(padrao.findall(caminho))

def validar_com_pandera(df, caminho):
    df['BusinessYear'] = pd.to_numeric(df['BusinessYear'], errors='coerce')
    df['IssuerId'] = pd.to_numeric(df['IssuerId'], errors='coerce')

    if df['BusinessYear'].isnull().any():
        logging.warning(f"Valores inválidos encontrados em 'BusinessYear' no arquivo {caminho}")
    elif df['IssuerId'].isnull().any():
        logging.warning(f"Valores inválidos encontrados em 'IssuerId' no arquivo {caminho}")

    try:
        schema.validate(df, lazy=True)
        logging.info(f"Schema validado com sucesso para {caminho}")
        return True
    except pa.errors.SchemaErrors as err:
        logging.error(f"Erro de schema em {caminho}: {err.failure_cases}")
        return False

def processar_parquet(key):
    logging.info(f"Processando arquivo: {key}")

    with tempfile.NamedTemporaryFile() as tmp:
        try:
            s3.download_fileobj(BUCKET_SILVER, key, tmp)
            tmp.seek(0)
            df = pd.read_parquet(tmp.name)
            logging.info(f"Arquivo carregado: {df.shape[0]} linhas.")
        except Exception as e:
            logging.error(f"Erro ao ler '{key}': {str(e)}")
            return

        particoes = extrair_particoes(key)
        for coluna, valor in particoes.items():
            if coluna not in df.columns:
                df[coluna] = valor

        valido = validar_com_pandera(df, key)

        if valido:
            try:
                tmp_output = tempfile.NamedTemporaryFile()
                df.to_parquet(tmp_output.name, index=False)
                tmp_output.seek(0)
                key_gold = key.replace(PREFIX_SILVER, PREFIX_GOLD)

                s3.upload_fileobj(tmp_output, BUCKET_GOLD, key_gold)
                logging.info(f"Salvo em: s3://{BUCKET_GOLD}/{key_gold}")
            except Exception as e:
                logging.error(f"Erro ao salvar '{key_gold}': {str(e)}")
        else:
            logging.warning(f"Arquivo {key} inválido, não salvo.")

def lambda_handler(event, context):
    logging.info(f"Evento recebido: {event}")

    response = s3.list_objects_v2(Bucket=BUCKET_SILVER, Prefix=PREFIX_SILVER)

    if 'Contents' in response:
        keys = [item['Key'] for item in response['Contents'] if item['Key'].endswith('.parquet')]
        logging.info(f"Total de arquivos encontrados: {len(keys)}")

        for key in keys:
            processar_parquet(key)

    logging.info("Processamento finalizado.")

    return {
        'statusCode': 200,
        'body': 'Processamento concluído com sucesso.'
    }
