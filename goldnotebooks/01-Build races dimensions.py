# Databricks notebook source
# MAGIC %run ../00-common/01.enviromnnemt-config

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_races"

# COMMAND ----------

from pyspark.sql import functions as f 
circuits_df = spark.table(f"{catalog_name}.{silver_schema}.circuits")
races_df = spark.table(f"{catalog_name}.{silver_schema}.races")

# COMMAND ----------

dim_races = (
    circuits_df.join(
        races_df,
        circuits_df.circuits_ID == races_df.circuit_id,
        "inner"
    )
    .select(
        races_df.season,
        races_df.round,
        races_df.race_name,
        races_df.race_date,
        circuits_df.circuits_Name,
        circuits_df.locality,
        circuits_df.country
    )
)

# COMMAND ----------

dim_final_races = (
    dim_races
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))