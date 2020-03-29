from pyspark.sql import SparkSession

# read from local system
spark = SparkSession.builder.appName('abc').getOrCreate()
df = spark.read.format("csv").option("header", "true").load("/home/vaidhervi/Downloads/export.csv")
df.show()

# write
df.write.format("delta").save("file:///home/vaidhervi/delta/export")

# append
df.write.format("delta").mode("append").save("file:///home/vaidhervi/delta/exportappend")

# we can also partition the table
df.write.format("delta").partitionBy("date").save("/delta/events")

# update
df.write.format("delta").mode("append").option("replaceWhere", "date > '2016-07-28' AND date <='2017-01-04'").save("file:///home/vaidhervi/delta/export")
# add column automatically
df.write.format("delta").mode("append").option("mergeSchema", "true").save("file:///home/vaidhervi/delta/exportappend")

# Read old version of data using time travel
print("######## Read old data using time travel ############")
df = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table")
df.show()