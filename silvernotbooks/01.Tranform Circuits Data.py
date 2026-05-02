# Databricks notebook source
# MAGIC %md
# MAGIC # Tranform circuits data 
# MAGIC 1. Read bronze cirucits table
# MAGIC 2. keep only the column required for analysis 
# MAGIC 3. standardize column names using snake case 
# MAGIC 4. rename columns to make them more meanigful 
# MAGIC 5. filter out rows where circuit_id is null 
# MAGIC 6. remove duplicate records 
# MAGIC 7. transform values of columns circuit_name and locality to title case 
# MAGIC 8. write the tranformed data to silver table 

# COMMAND ----------

# MAGIC %run ../00-common/01.enviromnnemt-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

# spark.read.table(bronze_table)

# COMMAND ----------

circuits_df = spark.table(bronze_table)

# COMMAND ----------

from pyspark.sql import functions as f 

# COMMAND ----------

circuits_df = spark.table(bronze_table).select(
    "circuitsID",
    "circuitname",
    "lat",
    "long",
    "locality",
    "country",
    "ingestion_timestamp",
    "Source_file"
)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_renamed_df = circuits_df.withColumnsRenamed(
    {
        "circuitsID": "circuits_ID",
        "circuitname": "circuits_Name",
        "lat": "latitude",
        "long": "longitute",
    }
)

# COMMAND ----------

circuits_value_df = circuits_renamed_df.filter(
    f.col("circuits_ID").isNotNull()
)

# COMMAND ----------

display(circuits_value_df)

# COMMAND ----------

circuits_distinct_df = circuits_value_df.dropDuplicates(["circuits_ID"])

# COMMAND ----------

display(circuits_distinct_df)

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df
        .withColumn("circuits_Name",f.initcap(f.col("circuits_Name")))
        .withColumn("locality",f.initcap(f.col("locality")))
)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

(
    circuits_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

