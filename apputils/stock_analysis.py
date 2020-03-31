import datetime
from pyspark.sql import SparkSession

# read from local system
spark = SparkSession.builder.appName('abc').getOrCreate()
dfBasePrice = spark.read.format("csv").option("header", "true").load("/home/vaidhervi/Downloads/stocksDailyPrices.csv")
dfBaseFund = spark.read.format("csv").option("header", "true").load("/home/vaidhervi/Downloads/stocksFundamentals.csv")
dfBaseFund.show()
dfBasePrice.show()

# write
# Create Fundamental Data (Databricks Delta table)
print("delta table")
dfBaseFund.write.mode('overwrite').format("delta").save("file:///home/vaidhervi/delta/stockF")
# Create Price Data (Databricks Delta table)
dfBasePrice.write.mode('overwrite').format("delta").save("file:///home/vaidhervi/delta/stockP")

print("transformations")
row = dfBasePrice.aggregate((dfBasePrice.price_date.max()).alias("maxDate"),
                            (dfBasePrice.price_date.min()).alias("minDate")).collect()[0]
startDate = row["minDate"]
endDate = row["maxDate"]


# Define our date range function
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


# Define combinePriceAndFund information by date
def combinePriceAndFund(theDate):
    dfFund = dfBaseFund.where(
        dfBaseFund.price_date == theDate)
    dfPrice = dfBasePrice.where(
        dfBasePrice.price_date == theDate).drop('price_date').collect()

    # Drop the updated column
    dfPriceWFund = dfPrice.join(dfFund, on=["ticker"], how='inner').drop("updated")

    # Save data to DBFS
    dfPriceWFund.write.format("delta").mode("append").save("file:///home/vaidhervi/delta/stocksDailyPricesWFund")


# Loop through dates to complete fundamentals
# + price ETL process
for single_date in daterange(startDate, (endDate + datetime.timedelta(days=1))):
    start = datetime.datetime.now()
    combinePriceAndFund(single_date)
    end = datetime.datetime.now()

dfPriceWithFundamentals = spark.read.format("delta").load("file:///home/vaidhervi/delta/stocksDailyPricesWFund")
dfPriceWithFundamentals.show()
# Create temporary view of the data
dfPriceWithFundamentals.createOrReplaceTempView("priceWithFundamentals")
