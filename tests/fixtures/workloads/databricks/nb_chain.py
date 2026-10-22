# Databricks notebook source
# MAGIC %run ./helpers/common

# COMMAND ----------

result = dbutils.notebook.run("/Shared/etl/child", 600, {"x": "1"})
try:
    dbutils.notebook.exit(result)
except Exception:
    pass

# COMMAND ----------

# MAGIC %pip install requests==2.31.0 pyyaml

# COMMAND ----------

import requests
import yaml
import os
from databricks.sdk import WorkspaceClient
