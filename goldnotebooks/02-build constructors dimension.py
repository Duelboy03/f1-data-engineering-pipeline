# Databricks notebook source
# MAGIC %run ../00-common/01.enviromnnemt-config

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

constructors_df = spark.table(f"{catalog_name}.{silver_schema}.constructors")
ref_nationality_region = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

dim_constructors = (
    constructors_df.join(
        ref_nationality_region,
        constructors_df.nationality == ref_nationality_region.nationality,
        "left"
    )
    .select(
        constructors_df.constructor_Id,
        constructors_df.constructor_name,
        constructors_df.nationality,
        ref_nationality_region.region.alias("nationality_region")
    )
)

# COMMAND ----------

(
    dim_constructors
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)