# Databricks notebook source
# MAGIC %md
# MAGIC # Daily ETL

# COMMAND ----------

dbutils.widgets.text("run_date", "2024-01-01", "Run date")
dbutils.widgets.text("source_folder", "landing")
run_date = dbutils.widgets.get("run_date")
folder = dbutils.widgets.get("source_folder")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# COMMAND ----------

key = dbutils.secrets.get("kv-scope", "storage-key")
files = dbutils.fs.ls("abfss://raw@acctone.dfs.core.windows.net/" + folder)
df = spark.read.parquet("abfss://raw@acctone.dfs.core.windows.net/" + folder)
df.write.mode("overwrite").saveAsTable("curated.daily")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM curated.daily

# COMMAND ----------

dbutils.notebook.exit("done:" + run_date)
