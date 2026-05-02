# f1-data-engineering-pipeline
This project builds an end-to-end data engineering pipeline using Formula 1 race data on Databricks. It follows the Medallion Architecture (Bronze, Silver, Gold) and implements incremental batch processing using Delta Lake.

##Tech Stack
* PySpark
* Databricks
* Delta Lake
* Unity Catalog
* SQL
* Databricks Workflows
  

 #  Pipeline Flow

 Bronze Layer

* Ingest raw Formula 1 data (CSV/JSON)
* Applied schema and basic validation
* Stored as Delta tables

 Silver Layer

* Cleaned and transformed data
* Joined multiple datasets (drivers, races, constructors)
* Standardized schema

 Gold Layer

* Built analytics-ready tables
* Driver standings
* Constructor performance
* Race insights

## Incremental Processing

* Implemented using Delta Lake MERGE
* Handles upserts efficiently without full reload

# Orchestration

* Automated using Databricks Workflows
* Scheduled pipeline execution

⸻
