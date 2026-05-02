# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Circuits.csv file
# MAGIC 1.Read the file using spark dataframe reader API 
# MAGIC 2.Add Metadata Columns
# MAGIC - Source File
# MAGIC - Ingestion Timestamp
# MAGIC 3. Write to Bronze delta table

# COMMAND ----------

# MAGIC %run ../00-common/01.enviromnnemt-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/circuits.csv"
table_path = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Read the csv file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,DoubleType

circuits_schema = StructType([
    StructField('circuitsId',   StringType()),
    StructField('url',          StringType()),
    StructField('circuitName',  StringType()),
    StructField('lat',          DoubleType()),
    StructField('long',         DoubleType()),
    StructField('locality',     StringType()),
    StructField('country',      StringType())
])

# COMMAND ----------

circuits_df = (
    spark.read
    .format('csv')
    .option('header','true')
    #.option('inferSchema','True')
    .option('mode','FAILFAST')
    .schema(circuits_schema)
    .load(source_file)
)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Add metadata coloumn
# MAGIC - Source File
# MAGIC - Ingestion Timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_metadata(circuits_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## step 3 - write to bronze delta table

# COMMAND ----------

(
  circuits_final_df
  .write
  .format('delta')
  .mode('overwrite')
  .saveAsTable(table_path)
)

# COMMAND ----------

display(spark.table(table_path))

# COMMAND ----------

