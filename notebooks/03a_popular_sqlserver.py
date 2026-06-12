# Databricks notebook source
# MAGIC %md
# MAGIC # 03a - Popular SQL Server com dados operacionais

# COMMAND ----------

# MAGIC %pip install python-tds --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do SQL Server

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inicialização do Banco de Dados

# COMMAND ----------

import pytds

try:
    conn = pytds.connect(
        server=sqlserver_options["host"],
        database="brasilflow",
        user=sqlserver_options["user"],
        password=sqlserver_options["password"]
    )
    conn.close()
    print("INFO: O banco 'brasilflow' já existe.")
except Exception:
    print("INFO: O banco 'brasilflow' não existe. Criando...")
    try:
        conn = pytds.connect(
            server=sqlserver_options["host"],
            database="master",
            user=sqlserver_options["user"],
            password=sqlserver_options["password"],
            autocommit=True
        )
        with conn.cursor() as cursor:
            cursor.execute("CREATE DATABASE brasilflow")
        conn.close()
        print("INFO: Banco 'brasilflow' criado com sucesso!")
    except Exception as err:
        print(f"ERROR: Erro ao criar o banco: {err}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela 1: Motoristas

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import *

motoristas = spark.createDataFrame([
    Row(id=1,  nome="Carlos Alberto Silva",    cpf="123.456.789-00", data_nascimento="1985-03-15", data_admissao="2018-02-01", cnh="AB", status="ativo"),
    Row(id=2,  nome="Maria Fernanda Santos",   cpf="234.567.890-11", data_nascimento="1990-07-22", data_admissao="2019-06-15", cnh="D",  status="ativo"),
    Row(id=3,  nome="José Ricardo Oliveira",   cpf="345.678.901-22", data_nascimento="1978-11-08", data_admissao="2015-01-10", cnh="D",  status="ativo"),
    Row(id=4,  nome="Ana Paula Ferreira",      cpf="456.789.012-33", data_nascimento="1992-04-30", data_admissao="2020-03-20", cnh="D",  status="ativo"),
    Row(id=5,  nome="Roberto Carlos Mendes",   cpf="567.890.123-44", data_nascimento="1982-09-12", data_admissao="2016-08-05", cnh="D",  status="afastado"),
    Row(id=6,  nome="Luciana Borges Lima",     cpf="678.901.234-55", data_nascimento="1988-01-25", data_admissao="2017-11-18", cnh="D",  status="ativo"),
    Row(id=7,  nome="Fernando Souza Costa",    cpf="789.012.345-66", data_nascimento="1975-06-14", data_admissao="2010-04-22", cnh="D",  status="ativo"),
    Row(id=8,  nome="Patrícia Almeida Rocha",  cpf="890.123.456-77", data_nascimento="1995-12-03", data_admissao="2021-01-08", cnh="D",  status="ativo"),
    Row(id=9,  nome="Marcos Vinícius Pereira", cpf="901.234.567-88", data_nascimento="1980-08-19", data_admissao="2014-05-30", cnh="D",  status="desligado"),
    Row(id=10, nome="Juliana Martins Araujo",  cpf="012.345.678-99", data_nascimento="1993-02-07", data_admissao="2022-09-12", cnh="D",  status="ativo"),
    Row(id=11, nome="Antônio Marcos Ribeiro",  cpf="111.222.333-44", data_nascimento="1987-05-28", data_admissao="2019-03-01", cnh="D",  status="ativo"),
    Row(id=12, nome="Beatriz Souza Gomes",     cpf="222.333.444-55", data_nascimento="1991-10-16", data_admissao="2020-07-14", cnh="D",  status="ferias"),
])

motoristas.write \
    .format("sqlserver") \
    .option("dbtable", "motoristas") \
    .options(**sqlserver_options) \
    .mode("overwrite") \
    .save()
print(f"INFO: Tabela motoristas: {motoristas.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela 2: Escalas

# COMMAND ----------

escalas = spark.createDataFrame([
    Row(id=1,  motorista_id=1,  codigo_linha="6450-10", data="2026-06-09", turno="manha",  hora_inicio="05:00", hora_fim="13:00"),
    Row(id=2,  motorista_id=2,  codigo_linha="6450-10", data="2026-06-09", turno="tarde",  hora_inicio="13:00", hora_fim="21:00"),
    Row(id=3,  motorista_id=3,  codigo_linha="8700-10", data="2026-06-09", turno="manha",  hora_inicio="05:30", hora_fim="13:30"),
    Row(id=4,  motorista_id=4,  codigo_linha="8700-10", data="2026-06-09", turno="tarde",  hora_inicio="13:30", hora_fim="21:30"),
    Row(id=5,  motorista_id=6,  codigo_linha="2012-10", data="2026-06-09", turno="manha",  hora_inicio="05:00", hora_fim="13:00"),
    Row(id=6,  motorista_id=7,  codigo_linha="2012-10", data="2026-06-09", turno="noite",  hora_inicio="21:00", hora_fim="05:00"),
    Row(id=7,  motorista_id=8,  codigo_linha="5108-10", data="2026-06-09", turno="manha",  hora_inicio="05:00", hora_fim="13:00"),
    Row(id=8,  motorista_id=11, codigo_linha="5108-10", data="2026-06-09", turno="tarde",  hora_inicio="13:00", hora_fim="21:00"),
    Row(id=9,  motorista_id=1,  codigo_linha="6450-10", data="2026-06-10", turno="manha",  hora_inicio="05:00", hora_fim="13:00"),
    Row(id=10, motorista_id=3,  codigo_linha="8700-10", data="2026-06-10", turno="manha",  hora_inicio="05:30", hora_fim="13:30"),
    Row(id=11, motorista_id=4,  codigo_linha="2012-10", data="2026-06-10", turno="tarde",  hora_inicio="13:00", hora_fim="21:00"),
    Row(id=12, motorista_id=6,  codigo_linha="5108-10", data="2026-06-10", turno="manha",  hora_inicio="05:00", hora_fim="13:00"),
    Row(id=13, motorista_id=7,  codigo_linha="6450-10", data="2026-06-10", turno="noite",  hora_inicio="21:00", hora_fim="05:00"),
    Row(id=14, motorista_id=8,  codigo_linha="8700-10", data="2026-06-10", turno="tarde",  hora_inicio="13:30", hora_fim="21:30"),
    Row(id=15, motorista_id=11, codigo_linha="2012-10", data="2026-06-10", turno="manha",  hora_inicio="05:00", hora_fim="13:00"),
])

escalas.write \
    .format("sqlserver") \
    .option("dbtable", "escalas") \
    .options(**sqlserver_options) \
    .mode("overwrite") \
    .save()
print(f"INFO: Tabela escalas: {escalas.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela 3: Manutenção de Veículos

# COMMAND ----------

manutencao = spark.createDataFrame([
    Row(id=1,  prefixo_veiculo=71166, tipo="preventiva",  data="2026-05-10", custo=1250.00, descricao="Troca de óleo e filtros",           status="concluida"),
    Row(id=2,  prefixo_veiculo=71166, tipo="corretiva",   data="2026-06-01", custo=3800.50, descricao="Substituição do sistema de freios",  status="concluida"),
    Row(id=3,  prefixo_veiculo=72034, tipo="preventiva",  data="2026-05-15", custo=980.00,  descricao="Revisão geral do motor",             status="concluida"),
    Row(id=4,  prefixo_veiculo=72034, tipo="corretiva",   data="2026-06-05", custo=5200.00, descricao="Reparo na transmissão",              status="em_andamento"),
    Row(id=5,  prefixo_veiculo=73100, tipo="preventiva",  data="2026-04-20", custo=750.00,  descricao="Alinhamento e balanceamento",        status="concluida"),
    Row(id=6,  prefixo_veiculo=73100, tipo="preventiva",  data="2026-05-20", custo=1100.00, descricao="Troca de pastilhas de freio",        status="concluida"),
    Row(id=7,  prefixo_veiculo=74200, tipo="corretiva",   data="2026-06-08", custo=8500.00, descricao="Motor superaquecido - reparo bomba", status="em_andamento"),
    Row(id=8,  prefixo_veiculo=74200, tipo="preventiva",  data="2026-03-15", custo=650.00,  descricao="Inspeção elétrica",                  status="concluida"),
    Row(id=9,  prefixo_veiculo=75050, tipo="preventiva",  data="2026-05-25", custo=1400.00, descricao="Troca de pneus dianteiros",          status="concluida"),
    Row(id=10, prefixo_veiculo=75050, tipo="corretiva",   data="2026-06-07", custo=2300.00, descricao="Vazamento no sistema hidráulico",    status="concluida"),
    Row(id=11, prefixo_veiculo=76300, tipo="preventiva",  data="2026-06-01", custo=900.00,  descricao="Revisão ar condicionado",            status="concluida"),
    Row(id=12, prefixo_veiculo=76300, tipo="preventiva",  data="2026-04-10", custo=500.00,  descricao="Troca de lâmpadas e sinalização",    status="concluida"),
])

manutencao.write \
    .format("sqlserver") \
    .option("dbtable", "manutencao_veiculos") \
    .options(**sqlserver_options) \
    .mode("overwrite") \
    .save()
print(f"INFO: Tabela manutencao_veiculos: {manutencao.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela 4: Ocorrências

# COMMAND ----------

ocorrencias = spark.createDataFrame([
    Row(id=1,  prefixo_veiculo=71166, codigo_linha="6450-10", tipo="atraso",          data="2026-06-08", descricao="Atraso de 25 min por congestionamento na Marginal Pinheiros",    gravidade="media"),
    Row(id=2,  prefixo_veiculo=72034, codigo_linha="8700-10", tipo="acidente",        data="2026-06-07", descricao="Colisão leve com veículo particular na Av. Paulista",            gravidade="alta"),
    Row(id=3,  prefixo_veiculo=73100, codigo_linha="2012-10", tipo="pane_mecanica",   data="2026-06-06", descricao="Falha no sistema de ignição - veículo parou na via",             gravidade="alta"),
    Row(id=4,  prefixo_veiculo=71166, codigo_linha="6450-10", tipo="atraso",          data="2026-06-05", descricao="Atraso de 15 min por interdição na Rua da Consolação",           gravidade="baixa"),
    Row(id=5,  prefixo_veiculo=74200, codigo_linha="5108-10", tipo="reclamacao",      data="2026-06-08", descricao="Passageiro reportou ar condicionado desligado",                  gravidade="baixa"),
    Row(id=6,  prefixo_veiculo=75050, codigo_linha="2012-10", tipo="atraso",          data="2026-06-09", descricao="Atraso de 40 min por alagamento na região de Santo Amaro",       gravidade="alta"),
    Row(id=7,  prefixo_veiculo=76300, codigo_linha="8700-10", tipo="pane_mecanica",   data="2026-06-04", descricao="Problema na porta traseira - não fechava corretamente",          gravidade="media"),
    Row(id=8,  prefixo_veiculo=72034, codigo_linha="8700-10", tipo="desvio_rota",     data="2026-06-09", descricao="Desvio por obras na Av. Rebouças",                              gravidade="baixa"),
    Row(id=9,  prefixo_veiculo=73100, codigo_linha="6450-10", tipo="acidente",        data="2026-06-03", descricao="Ônibus atingido por motocicleta - sem vítimas",                  gravidade="media"),
    Row(id=10, prefixo_veiculo=74200, codigo_linha="5108-10", tipo="atraso",          data="2026-06-09", descricao="Atraso de 20 min por manifestação na Av. Brigadeiro Faria Lima", gravidade="media"),
])

ocorrencias.write \
    .format("sqlserver") \
    .option("dbtable", "ocorrencias") \
    .options(**sqlserver_options) \
    .mode("overwrite") \
    .save()
print(f"INFO: Tabela ocorrencias: {ocorrencias.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação: ler tudo de volta via JDBC

# COMMAND ----------

for tabela in ["motoristas", "escalas", "manutencao_veiculos", "ocorrencias"]:
    df = spark.read \
        .format("sqlserver") \
        .option("dbtable", tabela) \
        .options(**sqlserver_options) \
        .load()
    print(f"INFO: {tabela}: {df.count()} registros, {len(df.columns)} colunas")

# COMMAND ----------

for tabela in ["motoristas", "escalas", "manutencao_veiculos", "ocorrencias"]:
    print(f"\n{'='*50}")
    print(f"INFO: Tabela {tabela}")
    print(f"{'='*50}")
    df_amostra = spark.read \
        .format("sqlserver") \
        .option("dbtable", tabela) \
        .options(**sqlserver_options) \
        .load()
    display(df_amostra)
