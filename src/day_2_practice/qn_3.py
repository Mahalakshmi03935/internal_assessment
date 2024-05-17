from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("data").getOrCreate()
std_schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("student_name", StringType(), True)
])
marks_schema = StructType([
    StructField("course_name", StringType(), True),
    StructField("marks", IntegerType(), True),
    StructField("student_id", IntegerType(), True)
])
std_data = [(101, "Aravind"), (102, "John"), (103, "Sridevi")]
marks_data = [("pyspark", 90, 101), ("sql", 70, 101), ("pyspark", 70, 102), ("sql", 60, 102), ("sql", 30, 103), ("pyspark", 20, 103)]
std_df = spark.createDataFrame(std_data, schema=std_schema)
marks_df = spark.createDataFrame(marks_data, schema=marks_schema)
average_marks_df = marks_df.groupBy("student_id").agg(avg("marks").alias("percentage"))
results_df = average_marks_df.withColumn(
    "result",
    when(col("percentage") >= 70, "Distinction")
    .when((col("percentage") >= 60) & (col("percentage") < 70), "First Class")
    .when((col("percentage") >= 50) & (col("percentage") < 60), "Second Class")
    .when((col("percentage") >= 40) & (col("percentage") < 50), "Third Class")
    .otherwise("Fail")
)
result_df = results_df.join(std_df, "student_id").select("student_id", "student_name", "percentage", "result")
result_df.show()