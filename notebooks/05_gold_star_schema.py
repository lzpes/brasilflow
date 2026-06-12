# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Modelagem Dimensional Gold (Star Schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS brasilflow.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão Tempo (`dim_tempo`)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE brasilflow.gold.dim_tempo AS
# MAGIC SELECT DISTINCT
# MAGIC   CAST(date_format(timestamp_gps, 'yyyyMMddHH') AS INT) as tempo_sk,
# MAGIC   CAST(timestamp_gps AS DATE) as data_referencia,
# MAGIC   YEAR(timestamp_gps) as ano,
# MAGIC   MONTH(timestamp_gps) as mes,
# MAGIC   DAY(timestamp_gps) as dia,
# MAGIC   HOUR(timestamp_gps) as hora,
# MAGIC   CASE 
# MAGIC     WHEN HOUR(timestamp_gps) BETWEEN 5 AND 11 THEN 'Manhã'
# MAGIC     WHEN HOUR(timestamp_gps) BETWEEN 12 AND 17 THEN 'Tarde'
# MAGIC     WHEN HOUR(timestamp_gps) BETWEEN 18 AND 23 THEN 'Noite'
# MAGIC     ELSE 'Madrugada'
# MAGIC   END as periodo_dia
# MAGIC FROM brasilflow.silver.silver_gps_cleaned
# MAGIC WHERE timestamp_gps IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão Rotas (`dim_routes`)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE brasilflow.gold.dim_routes AS
# MAGIC SELECT 
# MAGIC   abs(hash(route_short_name)) as route_sk,
# MAGIC   route_short_name as codigo_linha,
# MAGIC   route_long_name as nome_linha,
# MAGIC   agency_name as agencia,
# MAGIC   route_type as tipo_rota
# MAGIC FROM brasilflow.silver.silver_routes_enriched;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão Motoristas (`dim_motoristas`)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE brasilflow.gold.dim_motoristas AS
# MAGIC SELECT
# MAGIC   id as motorista_sk,
# MAGIC   nome,
# MAGIC   cnh,
# MAGIC   status,
# MAGIC   data_admissao,
# MAGIC   floor(datediff(current_date(), data_admissao) / 365.25) as anos_empresa
# MAGIC FROM brasilflow.silver.silver_motoristas;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela Fato: Posições GPS (`fact_posicoes_gps`)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE brasilflow.gold.fact_posicoes_gps AS
# MAGIC SELECT 
# MAGIC   uuid() as posicao_id,
# MAGIC   
# MAGIC   abs(hash(gps.codigo_linha)) as route_sk,
# MAGIC   CAST(date_format(gps.timestamp_gps, 'yyyyMMddHH') AS INT) as tempo_sk,
# MAGIC   
# MAGIC   gps.prefixo_veiculo,
# MAGIC   gps.acessivel as veiculo_acessivel,
# MAGIC   gps.latitude,
# MAGIC   gps.longitude,
# MAGIC   
# MAGIC   current_timestamp() as load_timestamp
# MAGIC   
# MAGIC FROM brasilflow.silver.silver_gps_cleaned gps;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação Final do Modelo Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   d_route.agencia,
# MAGIC   d_time.periodo_dia,
# MAGIC   COUNT(f.posicao_id) as total_posicoes
# MAGIC FROM brasilflow.gold.fact_posicoes_gps f
# MAGIC JOIN brasilflow.gold.dim_routes d_route 
# MAGIC   ON f.route_sk = d_route.route_sk
# MAGIC JOIN brasilflow.gold.dim_tempo d_time 
# MAGIC   ON f.tempo_sk = d_time.tempo_sk
# MAGIC GROUP BY 
# MAGIC   d_route.agencia, 
# MAGIC   d_time.periodo_dia
# MAGIC ORDER BY 
# MAGIC   total_posicoes DESC;
