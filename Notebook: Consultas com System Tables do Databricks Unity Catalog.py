# Databricks notebook source
# MAGIC %sql
# MAGIC -- Todas as tabelas do seu ambiente
# MAGIC select * from system.information_schema.tables where table_owner <> 'System user'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Todas as colunas de cada tabela
# MAGIC select 
# MAGIC     c.table_name,
# MAGIC     array_join(collect_set(column_name), ',') as columns 
# MAGIC from 
# MAGIC     system.information_schema.columns c
# MAGIC inner join 
# MAGIC     system.information_schema.tables t 
# MAGIC     on c.table_name = t.table_name 
# MAGIC     and c.table_catalog = t.table_catalog
# MAGIC where 
# MAGIC     t.table_owner <> 'System user'
# MAGIC group by 
# MAGIC     c.table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quantidade de tabelas por schema e catalog
# MAGIC select table_catalog,table_schema,count(*) as qtdTables
# MAGIC from system.information_schema.tables where table_owner <> 'System user'
# MAGIC group by all;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Auditoria do seu ambiente
# MAGIC select * from system.access.audit

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ultimo acesso nas suas tabelas
# MAGIC select LastAccess.event_time as LastAcces,LastAccess.entity_type,LastAccess.created_by as WhoAccessed,* 
# MAGIC from system.information_schema.tables a
# MAGIC LEFT JOIN 
# MAGIC LATERAL (select max(b.event_time) as event_time, LAST(b.entity_type) as entity_type, LAST(b.created_by) as created_by
# MAGIC from system.access.table_lineage b where b.target_table_name = a.table_name) as LastAccess
# MAGIC where a.table_owner <> 'System user';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quem acessou sua tabela e quando?
# MAGIC select * from system.access.table_lineage
# MAGIC order by event_time desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Todos os clusters do ambiente
# MAGIC select cluster_source,count(*) as qtd from system.compute.clusters
# MAGIC group by all

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cluster mais custoso em DBUs
# MAGIC select b.cluster_name, sum(usage_quantity) as `DBUs Consumed` from system.billing.usage a 
# MAGIC inner join system.compute.clusters b on a.usage_metadata.cluster_id = b.cluster_id
# MAGIC where usage_metadata.cluster_id is not null
# MAGIC group by all
# MAGIC order by 2 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cluster mais custoso em USD
# MAGIC select b.cluster_name, sum(usage_quantity) as `DBUs Consumed`, (sum(usage_quantity) * max(c.pricing.default)) as TotalUSD 
# MAGIC from system.billing.usage a 
# MAGIC inner join system.compute.clusters b on a.usage_metadata.cluster_id = b.cluster_id
# MAGIC inner join system.billing.list_prices c on c.sku_name = a.sku_name
# MAGIC where usage_metadata.cluster_id is not null
# MAGIC and usage_start_time between '2023-11-01' and '2023-11-30'
# MAGIC group by all
# MAGIC order by 3 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- total em USD por mês
# MAGIC select month(usage_end_time) as mes,sum(usage_quantity) as `DBUs Consumed`, (sum(usage_quantity) * max(c.pricing.default)) as TotalUSD 
# MAGIC from system.billing.usage a 
# MAGIC inner join system.billing.list_prices c on c.sku_name = a.sku_name
# MAGIC group by all
# MAGIC order by 1 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Execuções do PREDICTIVE OPTIMIZATION
# MAGIC select * from  system.storage.predictive_optimization_operations_history;
