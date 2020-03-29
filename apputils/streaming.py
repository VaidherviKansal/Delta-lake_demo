import random
from pyspark.sql import SparkSession

# Create SparkContext
spark = SparkSession\
    .builder\
    .appName("streaming")\
    .master("local[*]") \
    .getOrCreate()

# Create a table(key, value) of some data
data = spark.range(1000)
data = data.withColumn("value", data.id + random.randint(0, 5000))
data.show()
data.write.format("delta").save("file:///home/vaidhervi/delta/stream")


