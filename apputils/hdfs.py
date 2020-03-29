from pyspark.sql import SparkSession

# read from local system
spark = SparkSession.builder.appName('abc').getOrCreate()
df = spark.read.format("csv").option("header", "true").load("/home/vaidhervi/Downloads/export.csv")
df.show()

# write
df.write.mode('overwrite').format("delta").save("hdfs://localhost:9000/test/export")

# append
df.write.format("delta").mode("append").save("hdfs://localhost:9000/test/exportappend")