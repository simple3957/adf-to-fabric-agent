# Migrated from Synapse notebook NB_Transform_Cab_Data.
# Joins sales with customers, totals Amount by City and Product. Reads and writes through the
# LH_Migration shortcut to migrationcontainer, under migration-test/spark-test/.
storage = "abfss://53d67e72-8124-4ca3-ae55-c4b266935faa@onelake.dfs.fabric.microsoft.com/f33fe93b-9b37-42aa-ba41-762247bb43a5/Files/migrationcontainer/migration-test/spark-test"

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
