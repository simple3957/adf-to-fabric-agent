# Migrated from Synapse notebook NB_Transform_Cab_Data.
# Joins sales with customers, totals Amount by City and Product. Reads and writes through the
# LH_Migration shortcut to migrationcontainer, under migration-test/spark-test/.
# Set these two for your workspace and Lakehouse (the GUIDs in their Fabric URLs).
WORKSPACE_ID = "<workspace-id>"
LAKEHOUSE_ID = "<lakehouse-id>"
storage = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Files/migrationcontainer/migration-test/spark-test"

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
