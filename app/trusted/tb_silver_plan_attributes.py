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
APP_NAME = "Plan_Attributes"
TABLE_NAME = "tb_silver_plan_attributes"

# Configuração AWS
s3_client = boto3.client('s3')
S3_BUCKET = 'raw-test-edb'  # Substitua pelo nome do seu bucket S3
S3_OUTPUT_BUCKET = 'cleaned-test-edb'
INPUT_PREFIX = f'{APP_NAME}'
OUTPUT_PREFIX = f'{TABLE_NAME}' ## mudar

# Lista de colunas a serem processadas
COLUMNS = [
    "BusinessYear",
    "StateCode",
    "IssuerId",
    "SourceName",
    "VersionNum",
    "ImportDate",
    "BenefitPackageId",
    "IssuerId2",
    "StateCode2",
    "MarketCoverage",
    "DentalOnlyPlan",
    "TIN",
    "StandardComponentId",
    "PlanMarketingName",
    "HIOSProductId",
    "HPID",
    "NetworkId",
    "ServiceAreaId",
    "FormularyId",
    "IsNewPlan",
    "PlanType",
    "MetalLevel",
    "UniquePlanDesign",
    "QHPNonQHPTypeId",
    "IsNoticeRequiredForPregnancy",
    "IsReferralRequiredForSpecialist",
    "SpecialistRequiringReferral",
    "PlanLevelExclusions",
    "IndianPlanVariationEstimatedAdvancedPaymentAmountPerEnrollee",
    "CompositeRatingOffered",
    "ChildOnlyOffering",
    "ChildOnlyPlanId",
    "WellnessProgramOffered",
    "DiseaseManagementProgramsOffered",
    "EHBPercentTotalPremium",
    "EHBPediatricDentalApportionmentQuantity",
    "IsGuaranteedRate",
    "SpecialtyDrugMaximumCoinsurance",
    "InpatientCopaymentMaximumDays",
    "BeginPrimaryCareCostSharingAfterNumberOfVisits",
    "BeginPrimaryCareDeductibleCoinsuranceAfterNumberOfCopays",
    "PlanEffictiveDate",
    "PlanExpirationDate",
    "OutOfCountryCoverage",
    "OutOfCountryCoverageDescription",
    "OutOfServiceAreaCoverage",
    "OutOfServiceAreaCoverageDescription",
    "NationalNetwork",
    "URLForEnrollmentPayment",
    "FormularyURL",
    "PlanId",
    "CSRVariationType",
    "IssuerActuarialValue",
    "MedicalDrugDeductiblesIntegrated",
    "MedicalDrugMaximumOutofPocketIntegrated",
    "MultipleInNetworkTiers",
    "FirstTierUtilization",
    "SecondTierUtilization",
    "SBCHavingaBabyDeductible",
    "SBCHavingaBabyCopayment",
    "SBCHavingaBabyCoinsurance",
    "SBCHavingaBabyLimit",
    "SBCHavingDiabetesDeductible",
    "SBCHavingDiabetesCopayment",
    "SBCHavingDiabetesCoinsurance",
    "SBCHavingDiabetesLimit",
    "MEHBInnTier1IndividualMOOP",
    "MEHBInnTier1FamilyPerPersonMOOP",
    "MEHBInnTier1FamilyPerGroupMOOP",
    "MEHBInnTier2IndividualMOOP",
    "MEHBInnTier2FamilyPerPersonMOOP",
    "MEHBInnTier2FamilyPerGroupMOOP",
    "MEHBOutOfNetIndividualMOOP",
    "MEHBOutOfNetFamilyPerPersonMOOP",
    "MEHBOutOfNetFamilyPerGroupMOOP",
    "MEHBCombInnOonIndividualMOOP",
    "MEHBCombInnOonFamilyPerPersonMOOP",
    "MEHBCombInnOonFamilyPerGroupMOOP",
    "DEHBInnTier1IndividualMOOP",
    "DEHBInnTier1FamilyPerPersonMOOP",
    "DEHBInnTier1FamilyPerGroupMOOP",
    "DEHBInnTier2IndividualMOOP",
    "DEHBInnTier2FamilyPerPersonMOOP",
    "DEHBInnTier2FamilyPerGroupMOOP",
    "DEHBOutOfNetIndividualMOOP",
    "DEHBOutOfNetFamilyPerPersonMOOP",
    "DEHBOutOfNetFamilyPerGroupMOOP",
    "DEHBCombInnOonIndividualMOOP",
    "DEHBCombInnOonFamilyPerPersonMOOP",
    "DEHBCombInnOonFamilyPerGroupMOOP",
    "TEHBInnTier1IndividualMOOP",
    "TEHBInnTier1FamilyPerPersonMOOP",
    "TEHBInnTier1FamilyPerGroupMOOP",
    "TEHBInnTier2IndividualMOOP",
    "TEHBInnTier2FamilyPerPersonMOOP",
    "TEHBInnTier2FamilyPerGroupMOOP",
    "TEHBOutOfNetIndividualMOOP",
    "TEHBOutOfNetFamilyPerPersonMOOP",
    "TEHBOutOfNetFamilyPerGroupMOOP",
    "TEHBCombInnOonIndividualMOOP",
    "TEHBCombInnOonFamilyPerPersonMOOP",
    "TEHBCombInnOonFamilyPerGroupMOOP",
    "MEHBDedInnTier1Individual",
    "MEHBDedInnTier1FamilyPerPerson",
    "MEHBDedInnTier1FamilyPerGroup",
    "MEHBDedInnTier1Coinsurance",
    "MEHBDedInnTier2Individual",
    "MEHBDedInnTier2FamilyPerPerson",
    "MEHBDedInnTier2FamilyPerGroup",
    "MEHBDedInnTier2Coinsurance",
    "MEHBDedOutOfNetIndividual",
    "MEHBDedOutOfNetFamilyPerPerson",
    "MEHBDedOutOfNetFamilyPerGroup",
    "MEHBDedCombInnOonIndividual",
    "MEHBDedCombInnOonFamilyPerPerson",
    "MEHBDedCombInnOonFamilyPerGroup",
    "DEHBDedInnTier1Individual",
    "DEHBDedInnTier1FamilyPerPerson",
    "DEHBDedInnTier1FamilyPerGroup",
    "DEHBDedInnTier1Coinsurance",
    "DEHBDedInnTier2Individual",
    "DEHBDedInnTier2FamilyPerPerson",
    "DEHBDedInnTier2FamilyPerGroup",
    "DEHBDedInnTier2Coinsurance",
    "DEHBDedOutOfNetIndividual",
    "DEHBDedOutOfNetFamilyPerPerson",
    "DEHBDedOutOfNetFamilyPerGroup",
    "DEHBDedCombInnOonIndividual",
    "DEHBDedCombInnOonFamilyPerPerson",
    "DEHBDedCombInnOonFamilyPerGroup",
    "TEHBDedInnTier1Individual",
    "TEHBDedInnTier1FamilyPerPerson",
    "TEHBDedInnTier1FamilyPerGroup",
    "TEHBDedInnTier1Coinsurance",
    "TEHBDedInnTier2Individual",
    "TEHBDedInnTier2FamilyPerPerson",
    "TEHBDedInnTier2FamilyPerGroup",
    "TEHBDedInnTier2Coinsurance",
    "TEHBDedOutOfNetIndividual",
    "TEHBDedOutOfNetFamilyPerPerson",
    "TEHBDedOutOfNetFamilyPerGroup",
    "TEHBDedCombInnOonIndividual",
    "TEHBDedCombInnOonFamilyPerPerson",
    "TEHBDedCombInnOonFamilyPerGroup",
    "IsHSAEligible",
    "HSAOrHRAEmployerContribution",
    "HSAOrHRAEmployerContributionAmount",
    "URLForSummaryofBenefitsCoverage",
    "PlanBrochure",
    "RowNumber",
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
            ('BenefitPackageId', pa.string()),
            ('IssuerId2', pa.int8()),
            ('StateCode2', pa.string()),
            ('MarketCoverage', pa.string()),
            ('DentalOnlyPlan', pa.string()),
            ('TIN', pa.string()),
            ('StandardComponentId', pa.string()),
            ('PlanMarketingName', pa.string()),
            ('HIOSProductId', pa.string()),
            ('HPID', pa.string()),
            ('NetworkId', pa.string()),
            ('ServiceAreaId', pa.string()),
            ('FormularyId', pa.string()),
            ('IsNewPlan', pa.string()),
            ('PlanType', pa.string()),
            ('MetalLevel', pa.string()),
            ('UniquePlanDesign', pa.string()),
            ('QHPNonQHPTypeId', pa.string()),
            ('IsNoticeRequiredForPregnancy', pa.string()),
            ('IsReferralRequiredForSpecialist', pa.string()),
            ('SpecialistRequiringReferral', pa.string()),
            ('PlanLevelExclusions', pa.string()),
            ('IndianPlanVariationEstimatedAdvancedPaymentAmountPerEnrollee', pa.string()),
            ('CompositeRatingOffered', pa.string()),
            ('ChildOnlyOffering', pa.string()),
            ('ChildOnlyPlanId', pa.string()),
            ('WellnessProgramOffered', pa.string()),
            ('DiseaseManagementProgramsOffered', pa.string()),
            ('EHBPercentTotalPremium', pa.string()),
            ('EHBPediatricDentalApportionmentQuantity', pa.string()),
            ('IsGuaranteedRate', pa.string()),
            ('SpecialtyDrugMaximumCoinsurance', pa.string()),
            ('InpatientCopaymentMaximumDays', pa.string()),
            ('BeginPrimaryCareCostSharingAfterNumberOfVisits', pa.string()),
            ('BeginPrimaryCareDeductibleCoinsuranceAfterNumberOfCopays', pa.string()),
            ('PlanEffictiveDate', pa.string()),
            ('PlanExpirationDate', pa.string()),
            ('OutOfCountryCoverage', pa.string()),
            ('OutOfCountryCoverageDescription', pa.string()),
            ('OutOfServiceAreaCoverage', pa.string()),
            ('OutOfServiceAreaCoverageDescription', pa.string()),
            ('NationalNetwork', pa.string()),
            ('URLForEnrollmentPayment', pa.string()),
            ('FormularyURL', pa.string()),
            ('PlanId', pa.string()),
            ('CSRVariationType', pa.string()),
            ('IssuerActuarialValue', pa.string()),
            ('MedicalDrugDeductiblesIntegrated', pa.string()),
            ('MedicalDrugMaximumOutofPocketIntegrated', pa.string()),
            ('MultipleInNetworkTiers', pa.string()),
            ('FirstTierUtilization', pa.string()),
            ('SecondTierUtilization', pa.string()),
            ('SBCHavingaBabyDeductible', pa.string()),
            ('SBCHavingaBabyCopayment', pa.string()),
            ('SBCHavingaBabyCoinsurance', pa.string()),
            ('SBCHavingaBabyLimit', pa.string()),
            ('SBCHavingDiabetesDeductible', pa.string()),
            ('SBCHavingDiabetesCopayment', pa.string()),
            ('SBCHavingDiabetesCoinsurance', pa.string()),
            ('SBCHavingDiabetesLimit', pa.string()),
            ('MEHBInnTier1IndividualMOOP', pa.string()),
            ('MEHBInnTier1FamilyPerPersonMOOP', pa.string()),
            ('MEHBInnTier1FamilyPerGroupMOOP', pa.string()),
            ('MEHBInnTier2IndividualMOOP', pa.string()),
            ('MEHBInnTier2FamilyPerPersonMOOP', pa.string()),
            ('MEHBInnTier2FamilyPerGroupMOOP', pa.string()),
            ('MEHBOutOfNetIndividualMOOP', pa.string()),
            ('MEHBOutOfNetFamilyPerPersonMOOP', pa.string()),
            ('MEHBOutOfNetFamilyPerGroupMOOP', pa.string()),
            ('MEHBCombInnOonIndividualMOOP', pa.string()),
            ('MEHBCombInnOonFamilyPerPersonMOOP', pa.string()),
            ('MEHBCombInnOonFamilyPerGroupMOOP', pa.string()),
            ('DEHBInnTier1IndividualMOOP', pa.string()),
            ('DEHBInnTier1FamilyPerPersonMOOP', pa.string()),
            ('DEHBInnTier1FamilyPerGroupMOOP', pa.string()),
            ('DEHBInnTier2IndividualMOOP', pa.string()),
            ('DEHBInnTier2FamilyPerPersonMOOP', pa.string()),
            ('DEHBInnTier2FamilyPerGroupMOOP', pa.string()),
            ('DEHBOutOfNetIndividualMOOP', pa.string()),
            ('DEHBOutOfNetFamilyPerPersonMOOP', pa.string()),
            ('DEHBOutOfNetFamilyPerGroupMOOP', pa.string()),
            ('DEHBCombInnOonIndividualMOOP', pa.string()),
            ('DEHBCombInnOonFamilyPerPersonMOOP', pa.string()),
            ('DEHBCombInnOonFamilyPerGroupMOOP', pa.string()),
            ('TEHBInnTier1IndividualMOOP', pa.string()),
            ('TEHBInnTier1FamilyPerPersonMOOP', pa.string()),
            ('TEHBInnTier1FamilyPerGroupMOOP', pa.string()),
            ('TEHBInnTier2IndividualMOOP', pa.string()),
            ('TEHBInnTier2FamilyPerPersonMOOP', pa.string()),
            ('TEHBInnTier2FamilyPerGroupMOOP', pa.string()),
            ('TEHBOutOfNetIndividualMOOP', pa.string()),
            ('TEHBOutOfNetFamilyPerPersonMOOP', pa.string()),
            ('TEHBOutOfNetFamilyPerGroupMOOP', pa.string()),
            ('TEHBCombInnOonIndividualMOOP', pa.string()),
            ('TEHBCombInnOonFamilyPerPersonMOOP', pa.string()),
            ('TEHBCombInnOonFamilyPerGroupMOOP', pa.string()),
            ('MEHBDedInnTier1Individual', pa.string()),
            ('MEHBDedInnTier1FamilyPerPerson', pa.string()),
            ('MEHBDedInnTier1FamilyPerGroup', pa.string()),
            ('MEHBDedInnTier1Coinsurance', pa.string()),
            ('MEHBDedInnTier2Individual', pa.string()),
            ('MEHBDedInnTier2FamilyPerPerson', pa.string()),
            ('MEHBDedInnTier2FamilyPerGroup', pa.string()),
            ('MEHBDedInnTier2Coinsurance', pa.string()),
            ('MEHBDedOutOfNetIndividual', pa.string()),
            ('MEHBDedOutOfNetFamilyPerPerson', pa.string()),
            ('MEHBDedOutOfNetFamilyPerGroup', pa.string()),
            ('MEHBDedCombInnOonIndividual', pa.string()),
            ('MEHBDedCombInnOonFamilyPerPerson', pa.string()),
            ('MEHBDedCombInnOonFamilyPerGroup', pa.string()),
            ('DEHBDedInnTier1Individual', pa.string()),
            ('DEHBDedInnTier1FamilyPerPerson', pa.string()),
            ('DEHBDedInnTier1FamilyPerGroup', pa.string()),
            ('DEHBDedInnTier1Coinsurance', pa.string()),
            ('DEHBDedInnTier2Individual', pa.string()),
            ('DEHBDedInnTier2FamilyPerPerson', pa.string()),
            ('DEHBDedInnTier2FamilyPerGroup', pa.string()),
            ('DEHBDedInnTier2Coinsurance', pa.string()),
            ('DEHBDedOutOfNetIndividual', pa.string()),
            ('DEHBDedOutOfNetFamilyPerPerson', pa.string()),
            ('DEHBDedOutOfNetFamilyPerGroup', pa.string()),
            ('DEHBDedCombInnOonIndividual', pa.string()),
            ('DEHBDedCombInnOonFamilyPerPerson', pa.string()),
            ('DEHBDedCombInnOonFamilyPerGroup', pa.string()),
            ('TEHBDedInnTier1Individual', pa.string()),
            ('TEHBDedInnTier1FamilyPerPerson', pa.string()),
            ('TEHBDedInnTier1FamilyPerGroup', pa.string()),
            ('TEHBDedInnTier1Coinsurance', pa.string()),
            ('TEHBDedInnTier2Individual', pa.string()),
            ('TEHBDedInnTier2FamilyPerPerson', pa.string()),
            ('TEHBDedInnTier2FamilyPerGroup', pa.string()),
            ('TEHBDedInnTier2Coinsurance', pa.string()),
            ('TEHBDedOutOfNetIndividual', pa.string()),
            ('TEHBDedOutOfNetFamilyPerPerson', pa.string()),
            ('TEHBDedOutOfNetFamilyPerGroup', pa.string()),
            ('TEHBDedCombInnOonIndividual', pa.string()),
            ('TEHBDedCombInnOonFamilyPerPerson', pa.string()),
            ('TEHBDedCombInnOonFamilyPerGroup', pa.string()),
            ('IsHSAEligible', pa.string()),
            ('HSAOrHRAEmployerContribution', pa.string()),
            ('HSAOrHRAEmployerContributionAmount', pa.string()),
            ('URLForSummaryofBenefitsCoverage', pa.string()),
            ('PlanBrochure', pa.string()),
            ('RowNumber', pa.string()),
            ('ingestDate', pa.timestamp('us')),
            ('partitionDate', pa.int32())
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
