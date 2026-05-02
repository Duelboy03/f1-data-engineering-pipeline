# Databricks notebook source
# MAGIC %md
# MAGIC # ingest the drivers json file to bronze delta
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/01.enviromnnemt-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/drivers.json"
table_path = f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

# define the schema 
from pyspark.sql.types import StructType,StructField,DateType ,StringType

new_name = StructType([
    StructField('givenName',    StringType()),
    StructField('familyName',   StringType()),
])

drivers_schema = StructType([
    StructField('driverId', StringType()),
    StructField('name', new_name),
    StructField('dateOfBirth',  DateType()),
    StructField('nationality',   StringType()),
    StructField('url',  StringType()),
])

# COMMAND ----------

drivers_df = (
    spark.read
    .format('json')
    .schema(drivers_schema)
    .option('mode','FAILFAST')
    .load(source_file)
)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_final_df =add_ingestion_metadata(drivers_df)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

(
    drivers_final_df
    .write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(table_path)
)

# COMMAND ----------

display(spark.table(table_path))

# COMMAND ----------

