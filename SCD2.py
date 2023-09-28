# Databricks notebook source
from pyspark.sql.functions import col
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Criando o primeiro data frame
list_data = [
    
    ["Afonso", 25],
    ["Joao", 25],
    ["Juliana", 25],
    ["Maria", 25]
    
    
]

columns = ["FIRST_NAME", "AGE"]

df = spark.createDataFrame(list_data, columns)
df.display()

# COMMAND ----------

# DBTITLE 1,Filtrando o data frame para escrever uma versão sem nenhum dado
path = "/FileStore/scd_2/testes/"
dbutils.fs.rm(path, True)


df_null = df.filter(col("FIRST_NAME").isNull())
df_null.write.format("delta").mode("overwrite").save(path)


# COMMAND ----------

# DBTITLE 1,Habilitando o CDF
spark.sql(f""" ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true) """)

# COMMAND ----------

# DBTITLE 1,Gravando um data frame populado
df.write.format("delta").mode("overwrite").save(path)

# COMMAND ----------

# DBTITLE 1,Realizando a leitura a partir da primeira versão criada
options = {
    
    "readChangeFeed": "true", 
    "startingVersion": 1
}


df = spark.read.format("delta").options(**options).load(path)
df.createOrReplaceTempView("vw")
df.display()

# COMMAND ----------

# DBTITLE 1,Criação de query versionando os dados
# MAGIC %sql
# MAGIC
# MAGIC with raw as (
# MAGIC
# MAGIC select *,
# MAGIC _commit_timestamp start_date,
# MAGIC cast(ifnull(lag(_commit_timestamp, 1) over (partition by first_name order by _commit_timestamp desc), "2999-12-31") as timestamp) as end_date
# MAGIC from vw
# MAGIC
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC distinct
# MAGIC first_name, 
# MAGIC age, 
# MAGIC _change_type as operation,
# MAGIC row_number() over (partition by first_name order by end_date asc) as version,
# MAGIC case 
# MAGIC   when end_date = '2999-12-31' and _change_type != 'delete' then true
# MAGIC   else false
# MAGIC end is_active,
# MAGIC start_date, 
# MAGIC end_date
# MAGIC
# MAGIC
# MAGIC from raw
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Sobrescrevendo os registros
list_data = [
    
    ["Daniel", 25]
    
    
]

columns = ["FIRST_NAME", "AGE"]

df = spark.createDataFrame(list_data, columns)
df.write.format("delta").mode("overwrite").save(path)

# COMMAND ----------

# DBTITLE 1,Realizando nova leitura para visualizar os logs do CDF
options = {
    
    "readChangeFeed": "true", 
    "startingVersion": 1
}


df = spark.read.format("delta").options(**options).load(path)
df.createOrReplaceTempView("vw")
df.display()

# COMMAND ----------

# DBTITLE 1,Modificações na query e validação dos registros
# MAGIC %sql
# MAGIC
# MAGIC with raw as (
# MAGIC
# MAGIC select *,
# MAGIC _commit_timestamp start_date,
# MAGIC cast(ifnull(lag(_commit_timestamp, 1) over (partition by first_name order by _commit_timestamp desc), "2999-12-31") as timestamp) as end_date
# MAGIC from vw
# MAGIC
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC distinct
# MAGIC first_name, 
# MAGIC age, 
# MAGIC _change_type as operation,
# MAGIC row_number() over (partition by first_name order by end_date asc) as version,
# MAGIC case 
# MAGIC   when end_date = '2999-12-31' and _change_type != 'delete' then true
# MAGIC   else false
# MAGIC end is_active,
# MAGIC start_date, 
# MAGIC end_date
# MAGIC
# MAGIC
# MAGIC from raw
# MAGIC where 1=1 
# MAGIC       and _change_type != 'delete'

# COMMAND ----------

# DBTITLE 1,Aplicação de merge
list_data = [
    
    ["Daniel", 26], 
    ["Maria", 30], 
    ["Sandra", 20]
    
    
]

columns = ["FIRST_NAME", "AGE"]

df_source = spark.createDataFrame(list_data, columns)
df_target = DeltaTable.forPath(spark, path)

matched_columns = "target.FIRST_NAME = source.FIRST_NAME"
(   
 df_target.alias("target")
 .merge(df_source.alias("source"), matched_columns)
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute()
)


# COMMAND ----------

# DBTITLE 1,Visualizando os dados após operação de merge
options = {
    
    "readChangeFeed": "true", 
    "startingVersion": 1
}


df = spark.read.format("delta").options(**options).load(path)
df.createOrReplaceTempView("vw")
df.display()

# COMMAND ----------

# DBTITLE 1,Ajustes finais na query
# MAGIC %sql
# MAGIC
# MAGIC with raw as (
# MAGIC
# MAGIC select *,
# MAGIC _commit_timestamp start_date,
# MAGIC cast(ifnull(lag(_commit_timestamp, 1) over (partition by first_name order by _commit_timestamp desc), "2999-12-31") as timestamp) as end_date
# MAGIC from vw
# MAGIC
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC distinct
# MAGIC first_name, 
# MAGIC age, 
# MAGIC _change_type as operation,
# MAGIC row_number() over (partition by first_name order by start_date asc) as version,
# MAGIC case 
# MAGIC   when end_date = '2999-12-31' and _change_type != 'delete' then true
# MAGIC   else false
# MAGIC end is_active,
# MAGIC start_date, 
# MAGIC end_date
# MAGIC
# MAGIC
# MAGIC from raw
# MAGIC
# MAGIC where 1=1 
# MAGIC       and _change_type not in ('delete', 'update_preimage')
