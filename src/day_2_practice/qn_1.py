from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.appName("data").getOrCreate()

data = [("John", "python, sql"),
    ("Aravind", "Java,SQL,HTML"),
    ("Sridevi", "Python,sql,pyspark")
]
df = spark.createDataFrame(data, ["Name", "Skills"])
df = df.withColumn("Skills", explode(split(df["Skills"], ",")))
df.show()