import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from dataprocessing.config import data_path_str

from .sparketl import ETLSpark

# Define the expected structure of the raw JSON
vehicle_schema = T.StructType(
    [
        T.StructField("COD_LINHA", T.StringType(), True),
        T.StructField("VEIC", T.StringType(), True),
        T.StructField("LAT", T.StringType(), True),
        T.StructField("LON", T.StringType(), True),
        T.StructField("DTHR", T.StringType(), True),
    ]
)


class TrustProcessing:
    def __init__(self, date):
        self.etlspark = ETLSpark()
        self.date = date

    def __call__(self, *args, **kwargs):
        self.perform()

    def perform(self):
        vehicles = self.vehicles_ingestion(self.date)
        self.save(vehicles, data_path_str("trusted", "vehicles"))

        busstops = self.bustops_ingestion(self.date)
        self.save(busstops, data_path_str("trusted", "busstops"))

        lines = self.lines_ingestion(self.date)
        self.save(lines, data_path_str("trusted", "lines"))

    def vehicles_ingestion(self, period: str):
        return (
            self.etlspark.sqlContext.read.schema(vehicle_schema)
            .json(data_path_str("raw", period, "veiculos"))
            .select(
                F.col("COD_LINHA").alias("line_code"),
                F.date_format(
                    F.unix_timestamp("dthr", "dd/MM/yyyy HH:mm:ss").cast("timestamp"),
                    "yyyy-MM-dd HH:mm:ss",
                ).alias("event_timestamp"),
                F.translate("LAT", ",", ".").cast("double").alias("latitude"),
                F.translate("LON", ",", ".").cast("double").alias("longitude"),
                F.col("VEIC").alias("vehicle"),
            )
            .withColumn("year", F.year("event_timestamp"))
            .withColumn("month", F.month("event_timestamp"))
            .withColumn("day", F.dayofmonth("event_timestamp"))
            .dropDuplicates()
        )

    def lines_ingestion(self, period: str) -> DataFrame:
        return (
            self.etlspark.extract(data_path_str("raw", period, "linhas"))
            .withColumn("service_category", F.col("categoria_servico"))
            .withColumn("line_name", F.col("nome"))
            .withColumn("line_code", F.col("cod"))
            .withColumn("color", F.col("nome_cor"))
            .withColumn("card_only", F.col("somente_cartao"))
            .drop("categoria_servico", "cod", "nome", "nome_cor", "somente_cartao")
            .dropDuplicates()
        )

    def bustops_ingestion(self, period: str) -> DataFrame:
        return (
            self.etlspark.extract(data_path_str("raw", period, "pontoslinha"))
            .withColumn("line_code", F.col("cod"))
            .withColumn("latitude", F.regexp_replace("lat", ",", "."))
            .withColumn("longitude", F.regexp_replace("lon", ",", "."))
            .withColumn("name", F.col("nome"))
            .withColumn("number", F.col("num"))
            .withColumn("line_way", F.col("sentido"))
            .withColumn("seq", F.col("seq"))
            .withColumn("type", F.col("tipo"))
            .drop("GRUPO", "cod", "lat", "lon", "nome", "num", "sentido", "tipo")
            .dropDuplicates()
        )

    def save(self, df: DataFrame, output: str):
        expected_partitions = {"year", "month", "day"}
        current_columns = set(df.columns)

        writer = df.write.mode("overwrite")

        if expected_partitions.issubset(current_columns):
            (
                df.repartition("year", "month", "day")
                .write.mode("overwrite")
                .partitionBy("year", "month", "day")
                .format("parquet")
                .save(output)
            )
        else:
            writer.format("parquet").save(output)
