from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce
from pyspark.sql.types import StructType, StringType, StructField

spark = SparkSession.builder.appName("data").getOrCreate()
schema = StructType([
    StructField("Name1", StringType(), True),
    StructField("Name2", StringType(), True),
    StructField("Name3", StringType(), True)
])
data = [("Aravind", None, None),
        ("John", None, None),
        (None, "Sridevi", None)]

df = spark.createDataFrame(data, schema=schema)
df_filtered_names = df.select(
    coalesce(col("Name1"), coalesce(col("Name2"), col("Name3"))).alias("Names")
)
df_filtered = df_filtered_names.filter(col("Names").isNotNull())
df_filtered.show()