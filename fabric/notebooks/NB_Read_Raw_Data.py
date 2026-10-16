# Migrated from Synapse notebook NB_Read_Raw_Data.
# Generates the migration-test source data. Writes through the LH_Migration shortcut to
# migrationcontainer (storage datamigrationadftofabric), under migration-test/spark-test/ so the
# Copy pipelines' files in migration-test/csv and /parquet are never overwritten.
# Set these two for your workspace and Lakehouse (the GUIDs in their Fabric URLs).
WORKSPACE_ID = "<workspace-id>"
LAKEHOUSE_ID = "<lakehouse-id>"
storage = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Files/migrationcontainer/migration-test/spark-test"

customers = [
    ("C001", "Asha", "Hyderabad"),
    ("C002", "Ravi", "Vijayawada"),
    ("C003", "Priya", "Bengaluru"),
    ("C004", "Arjun", "Chennai")
]

sales = [
    ("S001", "C001", "Laptop", 65000),
    ("S002", "C002", "Phone", 30000),
    ("S003", "C001", "Monitor", 18000),
    ("S004", "C003", "Keyboard", 2500),
    ("S005", "C004", "Laptop", 72000)
]

customers_df = spark.createDataFrame(customers, ["CustomerId", "CustomerName", "City"])
sales_df = spark.createDataFrame(sales, ["SaleId", "CustomerId", "Product", "Amount"])

customers_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(storage + "/csv/customers")
sales_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(storage + "/csv/sales")
customers_df.write.mode("overwrite").parquet(storage + "/parquet/customers")
sales_df.write.mode("overwrite").parquet(storage + "/parquet/sales")

print("Migration test CSV and Parquet data created successfully.")
