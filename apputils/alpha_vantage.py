from alpha_vantage.timeseries import TimeSeries
from pyspark.sql import SparkSession

# Your key here
key = 'GBHBKSUMGEA3KZHU'
# Chose your output format, or default to JSON (python dict)
ts = TimeSeries(key, output_format='pandas')

# Get the data, returns a tuple
# aapl_data is a pandas dataframe, aapl_meta_data is a dict
aapl_data, aapl_meta_data = ts.get_daily(symbol='AAPL')
print(aapl_data)

# Create SparkContext
spark = SparkSession \
    .builder \
    .appName("streaming") \
    .master("local[*]") \
    .getOrCreate()

df = spark.createDataFrame(aapl_data)
df.write.format("delta").save("file:///home/vaidhervi/delta/source")

