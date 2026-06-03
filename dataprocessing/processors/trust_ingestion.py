import re
import shutil
from pathlib import Path

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from dataprocessing.config import data_path, data_path_str

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
    REQUIRED_RAW_FILES = {
        "veiculos": "veiculos.json",
        "linhas": "linhas.json",
        "pontoslinha": "pontosLinha.json",
    }

    REQUIRED_COLUMNS = {
        "vehicles": {"COD_LINHA", "VEIC", "LAT", "LON", "DTHR"},
        "lines": {"cod", "nome", "categoria_servico", "nome_cor", "somente_cartao"},
        "busstops": {"cod", "lat", "lon", "nome", "num", "sentido", "seq", "tipo"},
    }

    DAY_PATTERN = re.compile(r"^(\d{4}_\d{2}_\d{2})_.*\.json$")

    def __init__(self, date):
        self.etlspark = ETLSpark()
        self.date = date

    def __call__(self, *args, **kwargs):
        self.perform()

    def perform(self):
        candidate_days = self.list_candidate_days(self.date)

        processed_days = []
        skipped_days = []

        for day in candidate_days:
            try:
                self.process_day(self.date, day)
                processed_days.append(day)
                print(f"Trusted day processed: {day}")
            except Exception as err:
                self.cleanup_trusted_day(day)
                skipped_days.append((day, str(err)))
                print(f"Trusted day skipped: {day}. Reason: {err}")

        print(
            f"Trusted ingestion completed for {self.date}. "
            f"Processed days: {len(processed_days)}. "
            f"Skipped days: {len(skipped_days)}."
        )

        if skipped_days:
            print("Skipped day details:")
            for day, reason in skipped_days:
                print(f" - {day}: {reason}")

    def process_day(self, period: str, day: str):
        self.validate_day_inputs(period, day)

        vehicles = self.vehicles_ingestion(period, day)
        lines = self.lines_ingestion(period, day)
        busstops = self.bustops_ingestion(period, day)

        self.assert_dataframe_not_empty(vehicles, "vehicles")
        self.assert_dataframe_not_empty(lines, "lines")
        self.assert_dataframe_not_empty(busstops, "busstops")

        self.save(vehicles, data_path_str("trusted", "vehicles"))
        self.save(lines, data_path_str("trusted", "lines"))
        self.save(busstops, data_path_str("trusted", "busstops"))

    def list_candidate_days(self, period: str):
        day_sets = []
        for folder in self.REQUIRED_RAW_FILES:
            folder_path = data_path("raw", period, folder)
            if not folder_path.exists():
                continue

            days = set()
            for file_path in folder_path.glob("*.json"):
                match = self.DAY_PATTERN.match(file_path.name)
                if match:
                    days.add(match.group(1))

            if days:
                day_sets.append(days)

        if not day_sets:
            return []

        return sorted(set().union(*day_sets))

    def validate_day_inputs(self, period: str, day: str):
        for folder, filename in self.REQUIRED_RAW_FILES.items():
            raw_file = data_path("raw", period, folder, f"{day}_{filename}")
            if not raw_file.exists():
                raise FileNotFoundError(f"Missing raw file: {raw_file}")
            if raw_file.stat().st_size == 0:
                raise ValueError(f"Empty raw file: {raw_file}")

    def vehicles_ingestion(self, period: str, day: str):
        src = data_path_str("raw", period, "veiculos", f"{day}_veiculos.json")

        base_df = self.etlspark.sqlContext.read.schema(vehicle_schema).json(src)
        self.assert_required_columns(base_df, self.REQUIRED_COLUMNS["vehicles"], "vehicles")

        return (
            base_df
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

    def lines_ingestion(self, period: str, day: str) -> DataFrame:
        src = data_path_str("raw", period, "linhas", f"{day}_linhas.json")

        base_df = self.etlspark.extract(src)
        self.assert_required_columns(base_df, self.REQUIRED_COLUMNS["lines"], "lines")

        return (
            base_df
            .withColumn("service_category", F.col("categoria_servico"))
            .withColumn("line_name", F.col("nome"))
            .withColumn("line_code", F.col("cod"))
            .withColumn("color", F.col("nome_cor"))
            .withColumn("card_only", F.col("somente_cartao"))
            .drop("categoria_servico", "cod", "nome", "nome_cor", "somente_cartao")
            .dropDuplicates()
        )

    def bustops_ingestion(self, period: str, day: str) -> DataFrame:
        src = data_path_str("raw", period, "pontoslinha", f"{day}_pontosLinha.json")

        base_df = self.etlspark.extract(src)
        self.assert_required_columns(base_df, self.REQUIRED_COLUMNS["busstops"], "busstops")

        if "itinerary_id" in base_df.columns:
            itinerary_col = F.col("itinerary_id")
        else:
            itinerary_col = F.col("sentido")

        return (
            base_df
            .withColumn("line_code", F.col("cod"))
            .withColumn("latitude", F.regexp_replace("lat", ",", "."))
            .withColumn("longitude", F.regexp_replace("lon", ",", "."))
            .withColumn("itinerary_id", itinerary_col)
            .withColumn("name", F.col("nome"))
            .withColumn("number", F.col("num"))
            .withColumn("line_way", F.col("sentido"))
            .withColumn("seq", F.col("seq"))
            .withColumn("type", F.col("tipo"))
            .select(
                "line_code",
                "itinerary_id",
                "latitude",
                "longitude",
                "name",
                "number",
                "line_way",
                "seq",
                "type",
                "year",
                "month",
                "day",
            )
            .dropDuplicates()
        )

    @staticmethod
    def assert_required_columns(df: DataFrame, required_columns: set, dataset_name: str):
        current_columns = set(df.columns)
        missing = sorted(required_columns - current_columns)
        if missing:
            raise ValueError(
                f"Invalid {dataset_name} dataset. Missing columns: {', '.join(missing)}"
            )

    @staticmethod
    def assert_dataframe_not_empty(df: DataFrame, dataset_name: str):
        if df.rdd.isEmpty():
            raise ValueError(f"No rows produced for {dataset_name}")

    @staticmethod
    def partition_day_paths(base_folder: str, day: str):
        year, month, day_of_month = day.split("_")
        month_int = str(int(month))
        day_int = str(int(day_of_month))

        return [
            Path(base_folder) / f"year={year}" / f"month={month}" / f"day={day_of_month}",
            Path(base_folder) / f"year={year}" / f"month={month_int}" / f"day={day_int}",
            Path(base_folder) / f"year={year}" / f"month={month}" / f"day={day_int}",
            Path(base_folder) / f"year={year}" / f"month={month_int}" / f"day={day_of_month}",
        ]

    def cleanup_trusted_day(self, day: str):
        outputs = [
            data_path_str("trusted", "vehicles"),
            data_path_str("trusted", "lines"),
            data_path_str("trusted", "busstops"),
        ]

        for output in outputs:
            for partition_path in self.partition_day_paths(output, day):
                shutil.rmtree(partition_path, ignore_errors=True)

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
