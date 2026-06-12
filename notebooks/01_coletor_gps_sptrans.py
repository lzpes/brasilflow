# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Coletor GPS SPTrans

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração

# COMMAND ----------

import requests
import json
import time
from datetime import datetime

TOKEN = dbutils.secrets.get(scope="brasilflow", key="sptrans_token")
S3_LANDING_PATH = "s3://lpz-project-bus/landing/gps/"
INTERVALO_COLETA = 30
TOTAL_COLETAS = 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções

# COMMAND ----------

def autenticar(sessao, token):
    url = f"http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token={token}"
    response = sessao.post(url)
    autenticado = response.text.strip().lower() == "true"
    if autenticado:
        print(f"[{datetime.now()}] INFO: Autenticado com sucesso")
    else:
        print(f"[{datetime.now()}] ERROR: Falha na autenticação")
    return autenticado

# COMMAND ----------

def coletar_posicoes(sessao):
    url = "http://api.olhovivo.sptrans.com.br/v2.1/Posicao"
    try:
        response = sessao.get(url, timeout=15)
        if response.status_code == 200:
            dados = response.json()
            return dados
        else:
            print(f"[{datetime.now()}] WARNING: Status {response.status_code}")
            return None
    except Exception as e:
        print(f"[{datetime.now()}] ERROR: Erro na coleta: {e}")
        return None

# COMMAND ----------

def salvar_no_s3(dados, caminho_base):
    agora = datetime.utcnow()
    
    particao = agora.strftime("ano=%Y/mes=%m/dia=%d")
    nome_arquivo = agora.strftime("gps_%Y%m%d_%H%M%S.json")
    
    caminho_completo = f"{caminho_base}{particao}/{nome_arquivo}"
    
    json_str = json.dumps(dados, ensure_ascii=False)
    
    dbutils.fs.put(caminho_completo, json_str, overwrite=True)
    
    total_veiculos = sum(linha.get("qv", 0) for linha in dados.get("l", []))
    total_linhas = len(dados.get("l", []))
    
    print(f"[{agora}] INFO: Salvo: {nome_arquivo} | {total_linhas} linhas | {total_veiculos} veículos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução - Loop de Coleta

# COMMAND ----------

sessao = requests.Session()

if not autenticar(sessao, TOKEN):
    raise Exception("Não foi possível autenticar na API SPTrans. Verifique o token.")

falhas_seguidas = 0

for i in range(TOTAL_COLETAS if TOTAL_COLETAS > 0 else 999999):
    
    dados = coletar_posicoes(sessao)
    
    if dados and "l" in dados:
        salvar_no_s3(dados, S3_LANDING_PATH)
        falhas_seguidas = 0
    else:
        falhas_seguidas += 1
        print(f"[{datetime.now()}] WARNING: Falha {falhas_seguidas}/3")
        
        if falhas_seguidas >= 3:
            print(f"[{datetime.now()}] INFO: Re-autenticando...")
            if autenticar(sessao, TOKEN):
                falhas_seguidas = 0
            else:
                print(f"[{datetime.now()}] ERROR: Re-autenticação falhou. Aguardando 60s...")
                time.sleep(60)
                continue
    
    if i < (TOTAL_COLETAS - 1) if TOTAL_COLETAS > 0 else True:
        time.sleep(INTERVALO_COLETA)

print(f"\nINFO: Coleta finalizada! {TOTAL_COLETAS} ciclos executados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar arquivos salvos

# COMMAND ----------

display(dbutils.fs.ls(S3_LANDING_PATH))
