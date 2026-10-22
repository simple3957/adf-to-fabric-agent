import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
print(sys.argv[1:])
spark.range(3).show()
