# Databricks notebook source
# MAGIC %md
# MAGIC # ingest the constructor json file to bronze delta
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/01.enviromnnemt-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/constructors.json"
table_path = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# define the schema 
constructors_schema = """
                    constructorId STRING, 
                    name STRING ,
                    nationality STRING,
                    url STRING
                    """

# COMMAND ----------

constructors_df = (
    spark.read
    .format('json')
    .schema(constructors_schema)
    .option('mode','FAILFAST')
    .load(source_file)
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_final_df =add_ingestion_metadata(constructors_df)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

constructors_final_df = spark.table(table_path).drop("constructorRef")

(
    constructors_final_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(table_path)
)

# COMMAND ----------

constructors_final_df = spark.table(table_path).drop("constructorRef")

# COMMAND ----------

constructors_table_path = spark.table(table_path).drop("constructorRef")

# COMMAND ----------

(
    constructors_table_path
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(table_path)
)

# COMMAND ----------

constructors_table_path = spark.table(table_path).drop("constructorRef")

(
    constructors_table_path
    .write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")   
    .saveAsTable(table_path)
)

# COMMAND ----------

display(spark.table(table_path))

# COMMAND ----------

