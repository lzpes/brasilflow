# Databricks notebook source
# MAGIC %md
# MAGIC # 03b - Ingestão de Dados Operacionais (SQL Server → Bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração JDBC

# COMMAND ----------

sqlserver_options = {
    "host": "SEU_ENDPOINT_AWS_RDS_AQUI",
    "port": "1433",
    "database": "brasilflow",
    "user": "SEU_USER",
    "password": "SUA_SENHA",
    "encrypt": "false",
    "trustservercertificate": "true"
}

tabelas_operacionais = [
    "motoristas",
    "escalas",
    "manutencao_veiculos",
    "ocorrencias"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestão Batch para a Camada Bronze

# COMMAND ----------

from pyspark.sql import functions as F

for tabela in tabelas_operacionais:
    print(f"INFO: Iniciando a leitura de '{tabela}' do SQL Server...")
    
    df = (
        spark.read
        .format("sqlserver")
        .option("dbtable", tabela)
        .options(**sqlserver_options)
        .load()
    )
    
    df_bronze = df \
        .withColumn("ingested_at", F.current_timestamp()) \
        .withColumn("source_system", F.lit("sql_server_rds"))
    
    tabela_destino = f"brasilflow.bronze.operacional_{tabela}"
    
    print(f"INFO: Gravando dados na tabela Bronze: {tabela_destino}...")
    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(tabela_destino)
    )
    
    print(f"INFO: Tabela '{tabela_destino}' carregada com sucesso.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação dos dados gravados na Bronze

# COMMAND ----------

for tabela in tabelas_operacionais:
    tabela_destino = f"brasilflow.bronze.operacional_{tabela}"
    df_verificacao = spark.table(tabela_destino)
    print(f"INFO: {tabela_destino}: {df_verificacao.count()} registros carregados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualização de Amostra dos Dados Ingeridos

# COMMAND ----------

display(spark.table("brasilflow.bronze.operacional_motoristas").limit(5))
