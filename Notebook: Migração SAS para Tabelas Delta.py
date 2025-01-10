# Databricks notebook source
# Cell 1: Criar widgets de comando para os parâmetros de entrada
dbutils.widgets.text("sas_file_dir", "/caminho/para/seu/diretorio", "SAS File Directory")
dbutils.widgets.text("sas_file_name", "", "SAS File Name (optional)")
dbutils.widgets.text("catalog", "seu_catalogo", "Catalog")
dbutils.widgets.text("schema", "seu_schema", "Schema")
dbutils.widgets.text("table_name", "", "Table Name (optional)")

# COMMAND ----------

# Cell 2: Obter os valores dos widgets
sas_file_dir = dbutils.widgets.get("sas_file_dir")
sas_file_name = dbutils.widgets.get("sas_file_name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table_name")

# Função para carregar e salvar um arquivo SAS como tabela Delta
def load_and_save_sas_file(sas_file_path, catalog, schema, table_name):
    # Carregar o arquivo SAS em um DataFrame
    df = spark.read.format("com.github.saurfang.sas.spark") \
        .load(sas_file_path)

    # Exibir o DataFrame carregado
    display(df)

    # Especificar o caminho Delta
    delta_path = f"/mnt/delta/{catalog}/{schema}/{table_name}"

    # Escrever o DataFrame como uma tabela Delta
    df.write.format("delta") \
        .mode("overwrite") \
        .save(delta_path)

    # Criar a tabela Delta no catálogo especificado
    spark.sql(f"""
        CREATE TABLE {catalog}.{schema}.{table_name}
        USING DELTA
        LOCATION '{delta_path}'
    """)

# Verificar se um nome de arquivo específico foi fornecido
if sas_file_name:
    # Construir o caminho completo do arquivo SAS
    sas_file_path = f"{sas_file_dir}/{sas_file_name}"
    # Usar o nome da tabela fornecido ou derivar do nome do arquivo
    table_name = table_name if table_name else sas_file_name.split(".")[0]
    # Carregar e salvar o arquivo SAS
    load_and_save_sas_file(sas_file_path, catalog, schema, table_name)
else:
    # Listar todos os arquivos SAS no diretório
    files = dbutils.fs.ls(sas_file_dir)
    sas_files = [f.path for f in files if f.path.endswith(".sas7bdat")]

    # Carregar e salvar cada arquivo SAS
    for sas_file_path in sas_files:
        # Derivar o nome da tabela do nome do arquivo
        table_name = sas_file_path.split("/")[-1].split(".")[0]
        load_and_save_sas_file(sas_file_path, catalog, schema, table_name)
