from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import *
import shutil


# Create SparkContext
spark = SparkSession.builder.appName('abc').getOrCreate()

# read csv file
print("############ Reading the table ###############")
df = spark.read.format("csv").option("header", "true").load("/home/vaidhervi/Downloads/export.csv")
df.show()

# write
df.write.format("delta").save("file:///home/vaidhervi/delta/export")

# Update table data
print("########## Overwrite the table ###########")
data = spark.range(5, 10)
data.write.format("delta").mode("overwrite").save("/tmp/delta-table")

df.write.format("delta").mode("overwrite").option("replaceWhere", "date >= '2016-07-28' AND date <= '2017-01-04'").save(
    "file:///home/vaidhervi/delta/export")

deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")

# Update every even value by adding 100 to it
print("########### Update to the table(add 100 to every even value) ##############")
deltaTable.update(
    condition=expr("id % 2 == 0"),
    set={"id": expr("id + 100")})

deltaTable.toDF().show()

# Delete every even value
print("######### Delete every even value ##############")
deltaTable.delete(condition=expr("id % 2 == 0"))
deltaTable.toDF().show()

# cleanup
shutil.rmtree("/tmp/delta-table")
