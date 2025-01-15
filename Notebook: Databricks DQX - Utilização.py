# Databricks notebook source
# DBTITLE 1,Cell 1: Importar bibliotecas necessárias
from dqx import DataQualityChecker
from pyspark.sql import SparkSession

# Inicializar SparkSession se necessário
# spark = SparkSession.builder.appName("DataQualityCheck").getOrCreate()

# COMMAND ----------

# DBTITLE 1,Cell 2: Carregar dados da camada bronze
# Substitua 'path_to_bronze_data' pelo caminho real dos dados na camada bronze
df_bronze = spark.read.format("delta").load("path_to_bronze_data")

# COMMAND ----------

# DBTITLE 1,Cell 3: Definir regras de qualidade
rules = [
    {"rule_name": "not_null", "column": "transaction_id", "rule": "NOT_NULL"},
    {"rule_name": "positive_value", "column": "amount", "rule": "GREATER_THAN", "value": 0},
    {"rule_name": "valid_date", "column": "date", "rule": "DATE_FORMAT", "format": "yyyy-MM-dd"}
]

# COMMAND ----------

# DBTITLE 1,Cell 4: Inicializar o DataQualityChecker
checker = DataQualityChecker()

# COMMAND ----------

# DBTITLE 1,Cell 5: Aplicar regras ao DataFrame
result = checker.check(df_bronze, rules)

# COMMAND ----------

# DBTITLE 1,Cell 6: Separar dados válidos e inválidos
valid_data = result.valid_data()
invalid_data = result.invalid_data()

# COMMAND ----------

# DBTITLE 1,Cell 7: Salvar dados válidos na camada silver
# Substitua 'path_to_silver_data' pelo caminho real dos dados na camada silver
valid_data.write.format("delta").mode("overwrite").save("path_to_silver_data")

# COMMAND ----------

# DBTITLE 1,Cell 8: Exibir dados válidos
display(valid_data)

# COMMAND ----------

# DBTITLE 1,Cell 9: Exibir dados inválidos
display(invalid_data)
