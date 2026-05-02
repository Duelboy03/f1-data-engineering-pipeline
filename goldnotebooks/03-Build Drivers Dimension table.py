# Databricks notebook source
# MAGIC %run ../00-common/01.enviromnnemt-config

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

drivers_df = spark.table(f"{catalog_name}.{silver_schema}.drivers")
ref_nationality_region = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

dim_drivers_df = (
    drivers_df.join(
        ref_nationality_region,
        drivers_df.nationality == ref_nationality_region.nationality,
        "left"
    )
    .select(
        drivers_df.driver_id,
        drivers_df.drivers_name,
        drivers_df.date_of_birth,
        drivers_df.nationality.alias("nationality_region"),
    )
)

# COMMAND ----------

(
    dim_drivers_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

