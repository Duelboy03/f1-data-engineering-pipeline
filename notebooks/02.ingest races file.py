# Databricks notebook source
# MAGIC %md
# MAGIC #ingest racers.csv file
# MAGIC 1.Read the file using spark dataframe reader API 
# MAGIC
# MAGIC 2.Add Metadata Columns
# MAGIC
# MAGIC - Source File
# MAGIC - Ingestion Timestamp
# MAGIC
# MAGIC 3.Write to Bronze delta table

# COMMAND ----------

# MAGIC %run ../00-common/01.enviromnnemt-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/races.csv"
table_path = f"{catalog_name}.{bronze_schema}.races"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DateType,StringType

races_schema = StructType([
    StructField('season',   IntegerType()),
    StructField('round',    IntegerType()),
    StructField('url',      StringType()),
    StructField('raceName', StringType()),
    StructField('date',     DateType()),
    StructField('circuitID', StringType())
]

)


# COMMAND ----------

races_df = (
    spark.read
    .format('csv')
    .option('header',True)
    .option('mode','FAILFAST')
    .schema(races_schema)
    .load(source_file)
)

# COMMAND ----------

display(races_df)

# COMMAND ----------

racers_final_df = add_ingestion_metadata(races_df)


# COMMAND ----------

display(racers_final_df)

# COMMAND ----------

(
    racers_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(table_path)
)

# COMMAND ----------

display(spark.table(table_path))

# COMMAND ----------

