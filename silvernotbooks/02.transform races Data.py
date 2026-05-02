# Databricks notebook source
# MAGIC %md
# MAGIC # Tranform races data 
# MAGIC 1. Read bronze races table
# MAGIC 2. keep only the column required for analysis (drop url column)
# MAGIC 3. standardize column names using snake case 
# MAGIC 4. rename columns to make them more meanigful 
# MAGIC 5. filter out rows where circuit_id is null 
# MAGIC 6. remove duplicate records 
# MAGIC 7. transform values of columns race_name to title case 
# MAGIC 8. write the tranformed data to silver table 

# COMMAND ----------

# MAGIC %run ../00-common/01.enviromnnemt-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

races_df = spark.table(bronze_table)

# COMMAND ----------

races_df = (
    spark.table(bronze_table).select(
        "season",
        "round",
        "raceName",
        "date",
        "circuitID",
        "Ingestion_Timestamp",
        "Source_file"
    )
)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_renamed_df = (
    races_df
        .withColumnsRenamed({
            "raceName": "race_name",
            "circuitID": "circuit_id",
            "date": "race_date"
        })
)

# COMMAND ----------

races_distinct_df = (
    races_renamed_df
        .dropDuplicates(["season","round"])
)

# COMMAND ----------

display(races_distinct_df)

# COMMAND ----------

races_final_df = (
    races_distinct_df
        .withColumn("race_name",f.initcap(f.col("race_name")))
)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

(
    races_final_df
    .write
    .format('delta')
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

# COMMAND ----------

