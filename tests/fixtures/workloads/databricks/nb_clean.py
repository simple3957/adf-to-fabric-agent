# Databricks notebook source
# A notebook that needs no code change.

# COMMAND ----------

df = spark.range(10)
df.write.mode("overwrite").format("delta").save("Files/out")
print(df.count())
