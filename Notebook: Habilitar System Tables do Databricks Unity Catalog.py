# Databricks notebook source
# DBTITLE 1,Cell 1: Criar widgets de comando para os parâmetros de entrada
dbutils.widgets.text("metastore_id", "", "Metastore ID")
dbutils.widgets.text("pat_token", "", "PAT Token")
dbutils.widgets.text("schema_name", "", "Schema Name")

# COMMAND ----------

# DBTITLE 1,Cell 2: Obter os valores dos widgets
metastore_id = dbutils.widgets.get("metastore_id")
pat_token = dbutils.widgets.get("pat_token")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

# DBTITLE 1,Cell 4: Habilitar as system tables (se necessário)
# Exemplo de como habilitar uma system table específica
# Note que a habilitação de system tables pode variar dependendo da configuração do seu metastore

# Definir a URL da API para habilitar uma system table específica
enable_url = f"https://adb-3850690659548746.6.azuredatabricks.net/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas/{schema_name}"

# Fazer a requisição PUT para habilitar a system table
enable_response = requests.put(enable_url, headers=headers)

# Verificar se a requisição foi bem-sucedida
if enable_response.status_code == 200:
    print(f"System table {schema_name} habilitada com sucesso.")
else:
    print(f"Erro ao habilitar a system table: {enable_response.status_code} - {enable_response.text}")

# COMMAND ----------

# DBTITLE 1,Cell 3: Listar e habilitar as system tables
import requests

# Definir a URL da API
url = f"https://adb-3850690659548746.6.azuredatabricks.net/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas/"
print(url)

# Definir os headers da requisição
headers = {
    "Authorization": f"Bearer {pat_token}",
    "Content-Type": "application/json"
}

# Fazer a requisição GET para listar as system tables
response = requests.get(url, headers=headers)

# Verificar se a requisição foi bem-sucedida
if response.status_code == 200:
    systemschemas = response.json().get('schemas', [])
    display(spark.createDataFrame(systemschemas))
else:
    print(f"Erro ao listar as system tables: {response.status_code} - {response.text}")
