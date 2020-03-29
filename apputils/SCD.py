from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
from pyspark.sql import functions as F

# read from local system
spark = SparkSession.builder.appName('abc').getOrCreate()
df_history = spark.read.format("csv").option("header", "true").load("/home/vaidhervi/Downloads/History.csv")
df_history.show()
df_inc = spark.read.format("csv").option("header", "true").load("/home/vaidhervi/Downloads/Incremental.csv")
df_inc.show()

# write
df_history.write.format("delta").save("file:///home/vaidhervi/delta/export")
df_inc.write.format("delta").save("file:///home/vaidhervi/delta/export")

df2 = df_history.withColumn("address",F.lower(F.col("address"))).drop("address")
df2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("file:///home/vaidhervi/delta/export23")
df2.show()
