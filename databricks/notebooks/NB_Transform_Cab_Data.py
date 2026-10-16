# Databricks notebook source
# Migrated from Synapse notebook NB_Transform_Cab_Data (same logic as fabric/notebooks/NB_Transform_Cab_Data.py).
# Joins sales with customers, totals Amount by City and Product. Reads and writes ADLS
# migrationcontainer/migration-test/spark-test/ (Unity Catalog external location migration_test_spark).
storage = "abfss://migrationcontainer@datamigrationadftofabric.dfs.core.windows.net/migration-test/spark-test"

sales_df = spark.read.parquet(storage + "/parquet/sales")
customers_df = spark.read.parquet(storage + "/parquet/customers")

result = (
    sales_df.join(customers_df, "CustomerId", "left")
    .groupBy("City", "Product")
    .sum("Amount")
    .withColumnRenamed("sum(Amount)", "TotalAmount")
)

display(result)

result.write.mode("overwrite").parquet(storage + "/processed/sales_by_city_product")

print("Transformation completed.")
