# Databricks notebook source
# MAGIC %run ../00-common/01.enviromnnemt-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

drivers_df = spark.table(bronze_table)

# COMMAND ----------

from pyspark.sql import functions as f 

# COMMAND ----------

 drivers_df = spark.table(bronze_table).drop(f.col("url"))

# COMMAND ----------

drivers_renamed_df = (
    drivers_df
    .withColumnsRenamed({
        "driverId": "driver_id",
        "dateOfBirth": "date_of_birth"
    })
)

# COMMAND ----------

from pyspark.sql.functions import concat_ws

drivers_concat_df = (
    drivers_renamed_df
    .withColumn("drivers_name",concat_ws(" ", f.initcap(f.col("name.givenName")), f.initcap(f.col("name.familyName")))).drop("name")
)

# COMMAND ----------

display(drivers_concat_df)

# COMMAND ----------

drivers_distinct_df = (
    drivers_concat_df
    .dropDuplicates(["driver_id"])
)

# COMMAND ----------

display(drivers_distinct_df)

# COMMAND ----------

drivers_final_df = (
    drivers_distinct_df
    .withColumn("nationality",f.initcap(f.col("nationality")))
)

# COMMAND ----------

(
    drivers_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

