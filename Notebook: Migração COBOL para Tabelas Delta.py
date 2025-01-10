# Databricks notebook source
# DBTITLE 1,Cell 1: Solicitação de Parâmetros Iniciais
# Solicitar parâmetros iniciais usando widgets
dbutils.widgets.text("caminho_arquivo", "", "Digite o caminho do arquivo VSAM:")
dbutils.widgets.text("nome_arquivo", "", "Digite o nome do arquivo VSAM:")
dbutils.widgets.text("schema_destino", "", "Digite o schema de destino:")
dbutils.widgets.text("nome_tabela_destino", "", "Digite o nome da tabela de destino:")

caminho_arquivo = dbutils.widgets.get("caminho_arquivo")
nome_arquivo = dbutils.widgets.get("nome_arquivo")
schema_destino = dbutils.widgets.get("schema_destino")
nome_tabela_destino = dbutils.widgets.get("nome_tabela_destino")

# COMMAND ----------

# DBTITLE 1,Cell 2: Leitura do Arquivo VSAM
# Leitura do arquivo VSAM
vsam_path = f"{caminho_arquivo}/{nome_arquivo}"

# Verificar se os campos estão nulos
if not caminho_arquivo or not nome_arquivo or not schema_destino or not nome_tabela_destino:
    raise ValueError("Todos os parâmetros devem ser preenchidos.")

# Supondo que o arquivo VSAM seja um arquivo CSV em EBCDIC
df_vsam = spark.read.format("csv").option("header", "true").option("encoding", "cp037").load(vsam_path)

# Exibir o DataFrame lido
display(df_vsam)

# COMMAND ----------

# DBTITLE 1,Cell 3: Escrita para Tabela Delta
# Escrever o DataFrame em uma tabela Delta com controle de erros
delta_table_path = f"{schema_destino}.{nome_tabela_destino}"

try:
    df_vsam.write.format("delta").mode("overwrite").saveAsTable(delta_table_path)
    print(f"Tabela Delta '{delta_table_path}' criada com sucesso.")
except Exception as e:
    raise RuntimeError(f"Erro ao criar a Tabela Delta '{delta_table_path}': {e}")
