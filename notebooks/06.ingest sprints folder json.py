# Databricks notebook source
# MAGIC %md
# MAGIC # ingest the drivers json file to bronze delta
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/01.enviromnnemt-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/sprints"
table_path = f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

# define the schema 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

sprints_schema = StructType([
    StructField('constructorId', StringType()),
    StructField('date', DateType()),
    StructField('driverId', StringType()),
    StructField('grid', IntegerType()),
    StructField('laps', IntegerType()),
    StructField('number', IntegerType()),
    StructField('points', FloatType()),
    StructField('position', IntegerType()),
    StructField('positionText', StringType()),
    StructField('raceName',     StringType()),
    StructField('round', IntegerType()),
    StructField('season', IntegerType()),
    StructField('status', StringType()),
    StructField('url', StringType())
])

# COMMAND ----------

sprints_df = (
    spark.read
    .format('json')
    .schema(sprints_schema)
    .option('mode','FAILFAST')
    .option('multiLine',True)
    .load(source_file)
)

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

sprints_final_df =add_ingestion_metadata(sprints_df)

# COMMAND ----------

display(sprints_final_df)

# COMMAND ----------

(
    sprints_final_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(table_path)
)

# COMMAND ----------

display(spark.table(table_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC season,
# MAGIC COUNT(*)
# MAGIC FROM formula1.bronze.sprints
# MAGIC GROUP BY season
# MAGIC ORDER BY season

# COMMAND ----------

