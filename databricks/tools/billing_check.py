# Databricks notebook source
# Measured DBU usage of given jobs, from the Unity Catalog system table system.billing.usage.
# Parameter job_ids: comma-separated job ids (ADF/Fabric activities create one job per run).
# Returns JSON via dbutils.notebook.exit: one row per job and SKU, with DBUs and the period covered.
import json

dbutils.widgets.text("job_ids", "")
dbutils.widgets.text("since", "2026-10-01")
ids = [i.strip() for i in dbutils.widgets.get("job_ids").split(",") if i.strip().isdigit()]
since = dbutils.widgets.get("since")

rows = spark.sql(f"""
    SELECT u.usage_metadata.job_id      AS job_id,
           u.usage_metadata.job_run_id  AS job_run_id,
           u.usage_metadata.cluster_id  AS cluster_id,
           u.sku_name,
           u.usage_unit,
           SUM(u.usage_quantity)        AS dbus,
           MIN(u.usage_start_time)      AS first_start,
           MAX(u.usage_end_time)        AS last_end
    FROM system.billing.usage u
    WHERE u.usage_date >= '{since}' AND u.usage_metadata.job_id IN ({",".join(f"'{i}'" for i in ids) or "''"})
    GROUP BY ALL
""").collect()

dbutils.notebook.exit(json.dumps([{k: (str(v) if v is not None else None) for k, v in r.asDict().items()} for r in rows]))
