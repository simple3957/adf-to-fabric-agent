# Databricks notebook source
df = spark.read.csv("dbfs:/mnt/raw/sales.csv")
other = spark.read.parquet("/mnt/curated/sales")
vol = spark.read.parquet("/Volumes/main/sales/landing/x")
dbutils.fs.mount(source="wasbs://c@a.blob.core.windows.net", mount_point="/mnt/x")
jdbc = spark.read.format("jdbc").option("url", "jdbc:sqlserver://sql-private.contoso.net:1433;database=d;password=NOTAREALPASSWORD99").load()
storage_account_key = "ZmFrZWZha2VmYWtlZmFrZWZha2VmYWtlZmFrZQ=="
spark.conf.set("fs.azure.account.key.acct.dfs.core.windows.net", storage_account_key)
