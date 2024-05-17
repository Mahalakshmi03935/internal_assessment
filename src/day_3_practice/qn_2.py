from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("").getOrCreate()
data = [
    ("John", "Apple", 10),
    ("Alice", "Apple", 20),
    ("John", "Banana", 12),
    ("Alice", "Banana", 14),
    ("Mike", "Apple", 15),
    ("Mike", "Banana", 17)
]
df = spark.createDataFrame(data, ["salesperson", "product", "quantity"])
pivot_df = df.groupBy("product").pivot("salesperson").sum("quantity")
pivot_df.show()
spark.stop()