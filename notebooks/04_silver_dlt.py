# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Transformações Silver com Delta Live Tables (DLT)

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Limpeza do GPS (Streaming)

# COMMAND ----------

@dlt.table(
  name="silver_gps_cleaned"
)
@dlt.expect_or_drop("valid_coordinates", "latitude IS NOT NULL AND longitude IS NOT NULL AND latitude >= -90 AND latitude <= 90 AND longitude >= -180 AND longitude <= 180")
@dlt.expect_or_drop("valid_timestamp", "timestamp_gps IS NOT NULL")
def silver_gps_cleaned():
    return (
        spark.readStream.table("brasilflow.bronze.streaming_gps_raw")
        .withColumn("timestamp_gps", col("timestamp_gps").cast("timestamp"))
        .withColumn("data_referencia", to_date(col("timestamp_gps")))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Limpeza de Tabelas Operacionais (Batch)

# COMMAND ----------

@dlt.table(
    name="silver_motoristas"
)
@dlt.expect_or_drop("valid_cpf", "cpf IS NOT NULL")
def silver_motoristas():
    return (
        spark.table("brasilflow.bronze.operacional_motoristas")
        .withColumn("data_nascimento", col("data_nascimento").cast("date"))
        .withColumn("data_admissao", col("data_admissao").cast("date"))
        .dropDuplicates(["id"])
    )

@dlt.table(
    name="silver_ocorrencias"
)
@dlt.expect_or_drop("valid_date", "data IS NOT NULL")
def silver_ocorrencias():
    return (
        spark.table("brasilflow.bronze.operacional_ocorrencias")
        .withColumn("data", col("data").cast("date"))
        .withColumn("tipo", upper(col("tipo")))
        .dropDuplicates(["id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Limpeza do GTFS (Rotas e Agências)

# COMMAND ----------

@dlt.table(
    name="silver_routes_enriched"
)
@dlt.expect_or_drop("valid_route", "route_id IS NOT NULL")
def silver_routes_enriched():
    routes = spark.table("brasilflow.bronze.gtfs_routes_raw")
    agency = spark.table("brasilflow.bronze.gtfs_agency_raw")
    
    return (
        routes.join(agency, "agency_id", "left")
        .select(
            routes["route_id"],
            routes["agency_id"],
            agency["agency_name"],
            routes["route_short_name"],
            routes["route_long_name"],
            routes["route_type"]
        )
        .dropDuplicates(["route_id"])
    )
