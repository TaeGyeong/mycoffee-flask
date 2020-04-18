from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

class read_test:
    spark = ""
    def __init__(self):
        super().__init__()
        sc = SparkContext.getOrCreate()
        self.spark = SparkSession(sparkContext=sc)

    def get_three_row(self):
        data = self.spark.read.csv(path="data/data.csv",
                                sep=",",
                                encoding="utf-8",
                                comment=None,
                                header=True,
                                inferSchema=True)
        data.show(n=5, truncate=False)
        return True