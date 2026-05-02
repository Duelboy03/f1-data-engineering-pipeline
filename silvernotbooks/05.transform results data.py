# Databricks notebook source
# MAGIC %run ../00-common/01.enviromnnemt-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

from pyspark.sql import functions as f

results_df = (
    spark
    .table(bronze_table)
    .select(
        "constructorId",
        "date",
        "driverId",
        "grid",
        "laps",
        "number",
        "points",
        "position",
        "positionText",
        "raceName",
        "round",
        "season",
        "status",
        "ingestion_timestamp",
        "Source_file"
    )
    .withColumnsRenamed({
        "constructorId": "constructor_id",
        "driverId": "driver_id",
        "raceName": "race_name",
        "positionText": "finish_position_text",
        "date": "race_date",
        "grid": "grid_position",
        "laps": "completed_laps",
        "number": "car_number",
        "position": "finish_position"

    })
)

# COMMAND ----------

results_final_df = (
    results_df
    .filter(
        f.col("season").isNotNull() & 
        f.col("round").isNotNull() & 
        f.col("constructor_id").isNotNull() & 
        f.col("driver_id").isNotNull()
    )
    .dropDuplicates(
        [
            "season","round","constructor_id","driver_id"
        ]
    )
    .withColumn(
        "race_name",f.initcap(f.col("race_name"))
    )
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

