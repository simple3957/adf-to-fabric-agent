# Databricks notebook source
import dlt


@dlt.table
def bronze():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("abfss://raw@acctone.dfs.core.windows.net/in")
