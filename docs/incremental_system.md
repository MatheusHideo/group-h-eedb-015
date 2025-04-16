# Documentação do Sistema Incremental de Processamento de Dados

## Visão Geral

Este sistema implementa um processo de ingestão incremental de dados, projetado para processar e armazenar eficientemente grandes volumes de dados de seguros de saúde. O sistema lê arquivos CSV de um bucket S3, processa-os em chunks, e armazena os resultados em formato Parquet em outro bucket S3.

## Funcionamento do Sistema Incremental

O sistema incremental funciona da seguinte maneira:

1. **Leitura de Dados**: Os arquivos CSV são lidos do S3 em chunks para otimizar o uso de memória.

2. **Processamento de Chunks**: Cada chunk é processado individualmente.

3. **Geração de Metadados**: Para cada chunk, são adicionados metadados:
   - `ingestDate`: Data e hora da ingestão
   - `partitionDate`: Data de partição (YYYYMMDD)
   - `version`: Hash MD5 gerado a partir dos dados do chunk

4. **Comparação com Dados Existentes**: O sistema verifica se já existem dados para a tabela em questão no bucket de saída.

5. **Identificação de Novos Registros**: 
   - Se existirem dados, o sistema compara o chunk atual com os dados existentes.
   - Registros únicos ou atualizados são identificados com base nas colunas de comparação (excluindo metadados).

6. **Atualização Incremental**:
   - Novos registros são adicionados.
   - Registros existentes são atualizados se o `partitionDate` do novo registro for mais recente.

7. **Remoção de Duplicatas**: Duplicatas são removidas, mantendo o registro mais antigo com base na `ingestDate`.

8. **Validação**: Os dados finais são validados, gerando estatísticas como contagem total de linhas e percentuais de valores nulos.

9. **Armazenamento**: Os dados processados são salvos em formato Parquet no bucket de saída.

## Componentes Principais

### Funções Auxiliares

- `normalize_file_name(file_name)`: Normaliza nomes de arquivos.
- `ensure_s3_directory(bucket, path)`: Garante a existência de um diretório no S3.
- `read_csv_from_s3(bucket, key, chunksize)`: Lê arquivos CSV do S3.
- `validate_data(df)`: Realiza validação básica dos dados.
- `generate_version_hash(df)`: Gera um hash de versão para os dados.

### Função Principal de Processamento

`process_and_save_file(bucket, key, table_name)`:

1. Lê o arquivo CSV em chunks.
2. Processa cada chunk:
   - Adiciona metadados.
   - Compara com dados existentes.
   - Identifica novos registros ou atualizações.
3. Combina novos dados com existentes.
4. Remove duplicatas.
5. Valida os dados finais.
6. Salva em formato Parquet no S3.

## Características do Sistema Incremental

- **Eficiência de Memória**: Processa dados em chunks, otimizando o uso de memória.
- **Detecção de Mudanças**: Identifica e processa apenas novos registros ou atualizações.
- **Manutenção de Histórico**: Mantém versões anteriores dos dados através do controle de versão.
- **Escalabilidade**: Projetado para lidar com grandes volumes de dados.
- **Rastreabilidade**: Adiciona metadados para rastrear a origem e o tempo de ingestão dos dados.

## Considerações Finais

Este sistema incremental oferece uma solução robusta para o processamento contínuo de grandes volumes de dados, garantindo eficiência, rastreabilidade e integridade dos dados ao longo do tempo.