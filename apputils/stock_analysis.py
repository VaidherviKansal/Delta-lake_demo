from pyspark.sql import SparkSession

# read from local system
spark = SparkSession.builder.appName('abc').getOrCreate()
dfBasePrice = spark.read.format("csv").option("header", "true").load("/home/vaidhervi/Downloads/stocksDailyPrices")
dfBaseFund = spark.read.format("csv").option("header", "true").load("/home/vaidhervi/Downloads/stocksFundamentals")
dfBaseFund.show()
dfBasePrice.show()

# write
# Create Fundamental Data (Databricks Delta table)
dfBaseFund.write.format("delta").load("file:///home/vaidhervi/delta/stocksFundamentals")
# Create Price Data (Databricks Delta table)
dfBasePrice.write.format("delta").load("file:///home/vaidhervi/delta/stocksDailyPrices")


row = dfBasePrice.agg(
    func.max(dfBasePrice.price_date).alias("maxDate"),
    func.min(dfBasePrice.price_date).alias("minDate")).collect()[0]
startDate = row["minDate"]
endDate = row["maxDate"]