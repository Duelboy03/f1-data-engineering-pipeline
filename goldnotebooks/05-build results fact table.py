# Databricks notebook source
# MAGIC %run ../00-common/01.enviromnnemt-config

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_results"

# COMMAND ----------

from pyspark.sql import functions as f

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
    .withColumn(
        "session_type",f.lit("RACE")
    )
    .drop("race_name","race_date","ingestion_timestamp","source_file")
)

# COMMAND ----------

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
    .withColumn(
        "session_type",f.lit("SPRINTS")
    )
    .drop("race_name","race_date","ingestion_timestamp","source_file")
)

# COMMAND ----------

fact_results = (
    results_df.unionByName(sprints_df)
    .withColumn("is_win",f.col("finish_position") == 1)
    .withColumn("is_podium",f.col("finish_position").between(1,3))
    .withColumn("has_points",f.col("points") > 0)
)

# COMMAND ----------

(
    fact_results
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).filter("season = 2025"))

# COMMAND ----------

