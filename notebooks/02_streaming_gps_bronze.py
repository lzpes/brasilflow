# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Streaming GPS → Bronze (Delta Lake)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema do JSON da SPTrans

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, ArrayType
)

schema_veiculo = StructType([
    StructField("p", IntegerType(), True),
    StructField("a", BooleanType(), True),
    StructField("ta", StringType(), True),
    StructField("py", DoubleType(), True),
    StructField("px", DoubleType(), True),
    StructField("sv", StringType(), True),
    StructField("is", StringType(), True),
])

schema_linha = StructType([
    StructField("c", StringType(), True),
    StructField("cl", IntegerType(), True),
    StructField("sl", IntegerType(), True),
    StructField("lt0", StringType(), True),
    StructField("lt1", StringType(), True),
    StructField("qv", IntegerType(), True),
    StructField("vs", ArrayType(schema_veiculo), True),
])

schema_completo = StructType([
    StructField("hr", StringType(), True),
    StructField("l", ArrayType(schema_linha), True),
])

print("INFO: Schema definido")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração

# COMMAND ----------

S3_LANDING_PATH = "s3://lpz-project-bus/landing/gps/"
CHECKPOINT_PATH = "s3://lpz-project-bus/checkpoints/gps_bronze/"
TABELA_DESTINO = "brasilflow.bronze.streaming_gps_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura com Auto Loader (Spark Structured Streaming)

# COMMAND ----------

from pyspark.sql import functions as F

df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "schema/")
    .schema(schema_completo)
    .load(S3_LANDING_PATH)
)

print("INFO: Stream configurado, aguardando dados...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformação: explodir arrays aninhados

# COMMAND ----------

df_flat = (
    df_stream
    .withColumn("linha", F.explode("l"))
    .withColumn("veiculo", F.explode("linha.vs"))
    .select(
        F.col("hr").alias("hora_referencia"),
        F.col("linha.c").alias("codigo_linha"),
        F.col("linha.cl").alias("id_linha"),
        F.col("linha.sl").alias("sentido"),
        F.col("linha.lt0").alias("terminal_origem"),
        F.col("linha.lt1").alias("terminal_destino"),
        F.col("linha.qv").alias("qtd_veiculos"),
        F.col("veiculo.p").alias("prefixo_veiculo"),
        F.col("veiculo.a").alias("acessivel"),
        F.col("veiculo.ta").alias("timestamp_gps"),
        F.col("veiculo.py").alias("latitude"),
        F.col("veiculo.px").alias("longitude"),
        F.current_timestamp().alias("ingestion_timestamp"),
        F.input_file_name().alias("source_file")
    )
)

print("INFO: Transformação configurada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrita em Delta Lake (Bronze)

# COMMAND ----------

query = (
    df_flat.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TABELA_DESTINO)
)

query.awaitTermination()
print("INFO: Dados gravados na Bronze com sucesso")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar os dados gravados

# COMMAND ----------

total = spark.table(TABELA_DESTINO).count()
print(f"INFO: Total de registros na Bronze: {total}")

# COMMAND ----------

display(spark.table(TABELA_DESTINO).limit(20))

# COMMAND ----------

display(
    spark.table(TABELA_DESTINO)
    .select("codigo_linha", "terminal_origem", "terminal_destino")
    .distinct()
    .orderBy("codigo_linha")
)
