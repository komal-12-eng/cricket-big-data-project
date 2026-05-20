# Databricks notebook source
import requests
import json
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
# spark.sql("USE CATALOG workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.cricket_api_project")

base_path = '/Volumes/workspace/default/cricket_api_project'


# COMMAND ----------

# MAGIC %md
# MAGIC ### calling cricket API

# COMMAND ----------

import requests
import json

API_KEY = "YOUR_API_KEY"

# api_url = f"https://api.cricapi.com/v1/currentMatches?apikey={API_KEY}&offset=0"
api_url = "https://api.cricapi.com/v1/currentMatches?apikey=d9efd47b-a102-4d25-a562-20cc1cb18e66&offset=0"

response = requests.get(api_url)
response.raise_for_status()

api_data = response.json()

print(api_data.keys())


# COMMAND ----------

print(json.dumps(api_data, indent=4)[:2000])

# COMMAND ----------

# MAGIC %md
# MAGIC #### SAVE RAW API Response in the Volumns

# COMMAND ----------

raw_file_path = f"{base_path}/current_matches_raw.json"

with open (raw_file_path, 'w') as f:
  json.dump(api_data, f)

print(f"Raws API data is sav at the following path: {raw_file_path}")

# COMMAND ----------

# DBTITLE 1,CREATE  Bronze Layer Dataframe or Table

bronze_data = [{
    "source_api":api_url,
    "raw_json":json.dumps(api_data),
    "ingestion_time":None
}]

bronze_schema = StructType([
    StructField("source_api", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("ingestion_time", TimestampType(), True)
])


bronze_df = spark.createDataFrame(bronze_data, schema=bronze_schema)\
    .withColumn("ingestion_time",current_timestamp())

display(bronze_df)

# COMMAND ----------



# COMMAND ----------

bronze_data

# COMMAND ----------

bronze_schema

# COMMAND ----------

# DBTITLE 1,Save the Bronze Table
bronze_df.write.format('delta').mode("overwrite").saveAsTable("workspace.default.cricket_bronze_current_matches")
print("Data saved to table cricket_bronze_current_matches")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


