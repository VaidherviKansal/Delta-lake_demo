class SparkIO:

    def __init__(self, spark):
        self.spark = spark

    def load_csv(self, config):
        """
        This function loads data from local file system.
        :param config: configuration of spark session
        :return: status and data frame
        """
        try:
            df = self.spark\
                .read.format("csv")\
                .option("header", "true")\
                .load(config["/home/vaidhervi/Downloads/export.csv/"])
            return True, df
        except Exception as e:
            self.error("Error loading data. - {0}".format(e))
            return False, None
