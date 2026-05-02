# Databricks notebook source
# MAGIC %run ../00-common/01.enviromnnemt-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

constructors_df = (
    spark.table(bronze_table)
)

# COMMAND ----------

constructors_df = (
    spark.table(bronze_table).select(
        "constructorId",
        "name",
        "nationality",
        "ingestion_timestamp",
        "Source_file"
    )
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_renamed_df = (
    constructors_df
    .withColumnRenamed("constructorId", "constructor_Id")
    .withColumnRenamed("name", "constructor_name")
)

# COMMAND ----------

constructor_distinct_df = (
    constructors_renamed_df
    .dropDuplicates(["constructor_Id"])
)

# COMMAND ----------

display(constructor_distinct_df)

# COMMAND ----------

constructors_final_df = (
    constructor_distinct_df
    .withColumn("nationality",f.initcap(f.col("nationality")))
)

# COMMAND ----------

(
    constructors_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

