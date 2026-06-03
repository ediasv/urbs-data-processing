import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext


class ETLSpark:
    def __init__(self):
        # Only set the app name and let spark-submit handle all the hardware config
        self.conf = SparkConf().setAppName("URBS_ETL")
        self.conf = self.conf.set(
            "spark.sql.sources.partitionOverwriteMode", "dynamic"
        ).set("spark.sql.autoBroadcastJoinThreshold", "-1")
        self.sc = SparkContext.getOrCreate(conf=self.conf)
        self.sqlContext = SQLContext(self.sc)

    def extract(self, src):
        df = self.sqlContext.read.json(src).withColumn("filepath", F.input_file_name())

        df = df.withColumn(
            "filename",
            F.regexp_extract(F.col("filepath"), r"([^/]+)$", 1),
        )

        split = F.split(df["filename"], "_")

        df = df.withColumn("year", split.getItem(0))
        df = df.withColumn("month", split.getItem(1))
        df = df.withColumn("day", split.getItem(2))

        dropcolumns = ["filepath", "filename"]
        df = df.toDF(*[c.lower() for c in df.columns]).drop(*dropcolumns)

        return df
