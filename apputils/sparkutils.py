from pyspark.sql import SparkSession


def get_sparksession(config):
    """
    This function creates the spark session
    :param config: configuration - master and appname
    :return: spark object
    """
    spark = SparkSession.builder\
        .master(config["master"])\
        .appName(config["appname"])\
        .getOrCreate()
    return spark

# from pyspark.shell import spark
# from pyspark.sql.functions import expr
# from pyspark.sql.functions import from_unixtime
#
# events = spark.read \
#     .option("inferSchema", "true") \
#     .json("/home/vaidhervi/Downloads/export.csv/") \
#     .withColumn("date", expr("time")) \
#     .withColumn("date", from_unixtime("date", 'yyyy-MM-dd'))
#     # .drop("time") \


# print(events)