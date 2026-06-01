import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame
from pyspark.sql.window import Window

from dataprocessing.config import data_path_str

from .sparketl import ETLSpark


def interpolate_timestamp_expr(
    seq_col: Column,
    event_timestamp_col: Column,
    next_seq_col: Column,
    next_event_timestamp_col: Column,
) -> Column:
    s_ts = F.unix_timestamp(event_timestamp_col)
    n_ts = F.unix_timestamp(next_event_timestamp_col)
    delta_seq = next_seq_col - seq_col
    interpolated = s_ts + (n_ts - s_ts) / delta_seq
    return F.when(
        delta_seq != 0,
        F.to_timestamp(interpolated),
    ).otherwise(F.lit(None).cast(T.TimestampType()))


def haversine_distance_expr(
    lat1_col: Column,
    lon1_col: Column,
    lat2_col: Column,
    lon2_col: Column,
) -> Column:
    r_m = 6371000.0
    phi_1 = F.radians(lat1_col)
    phi_2 = F.radians(lat2_col)
    delta_phi = F.radians(lat2_col - lat1_col)
    delta_lambda = F.radians(lon2_col - lon1_col)
    a = F.pow(F.sin(delta_phi / 2.0), 2) + F.cos(phi_1) * F.cos(phi_2) * F.pow(
        F.sin(delta_lambda / 2.0), 2
    )
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    return F.round(F.lit(r_m) * c, 2)


class BusItineraryRefinedProcess:
    def __init__(self, year, month, day):
        self.etlspark = ETLSpark()
        self.bus_stops = self.filter_data(year, month, day)

    def perform(self):
        # Convert columns to numeric
        self.bus_stops = self.bus_stops.withColumn(
            "latitude", self.bus_stops["latitude"].cast("double")
        )
        self.bus_stops = self.bus_stops.withColumn(
            "longitude", self.bus_stops["longitude"].cast("double")
        )
        self.bus_stops = self.bus_stops.withColumn(
            "seq", self.bus_stops["seq"].cast("int")
        )

        # Sort values
        self.bus_stops = self.bus_stops.orderBy(["line_code", "itinerary_id", "seq"])

        # Add a row number column to ensure we can drop duplicates based on the first occurrence
        window_spec = Window.partitionBy(
            "line_code",
            "itinerary_id",
            "latitude",
            "longitude",
            "name",
            "number",
            "line_way",
            "type",
            "year",
            "month",
            "day",
        ).orderBy("seq")
        self.bus_stops = self.bus_stops.withColumn(
            "row_num", F.row_number().over(window_spec)
        )

        # Drop duplicates based on the tuple and keep the first occurrence
        self.bus_stops = self.bus_stops.filter(F.col("row_num") == 1).drop("row_num")

        # Select specific columns
        self.bus_stops = self.bus_stops.select(
            "line_code",
            "itinerary_id",
            "latitude",
            "longitude",
            "name",
            "number",
            "line_way",
            "type",
            "seq",
            "year",
            "month",
            "day",
        ).distinct()

        # Add 'id' column
        self.bus_stops = self.bus_stops.withColumn(
            "id", self.bus_stops["number"].cast("int")
        )

        # Add next_stop_id, next_stop_latitude, next_stop_longitude using window function
        window_spec = Window.partitionBy("line_code", "itinerary_id").orderBy("seq")

        self.bus_stops = self.bus_stops.withColumn(
            "next_stop_id", F.lag("id", -1).over(window_spec)
        )
        self.bus_stops = self.bus_stops.withColumn(
            "next_stop_latitude", F.lag("latitude", -1).over(window_spec)
        )
        self.bus_stops = self.bus_stops.withColumn(
            "next_stop_longitude", F.lag("longitude", -1).over(window_spec)
        )
        self.bus_stops = self.bus_stops.withColumn(
            "next_stop_delta_s",
            haversine_distance_expr(
                F.col("latitude"),
                F.col("longitude"),
                F.col("next_stop_latitude"),
                F.col("next_stop_longitude"),
            ),
        )

        # Filter rows where id != next_stop_id or next_stop_id is null
        self.bus_stops = self.bus_stops.filter(
            (F.col("id") != F.col("next_stop_id")) | F.col("next_stop_id").isNull()
        )

        # Add 'seq' and 'max_seq' columns using window function
        self.bus_stops = self.bus_stops.withColumn(
            "seq", F.row_number().over(window_spec) - 1
        )

        window_spec = Window.partitionBy("line_code", "itinerary_id")
        self.bus_stops = self.bus_stops.withColumn(
            "max_seq", F.max("seq").over(window_spec)
        )

        self.save(self.bus_stops, data_path_str("refined", "bus_itineraries"))

    def __call__(self, *args, **kwargs):
        self.perform()

    def filter_data(self, year: str, month: str, day: str) -> DataFrame:
        return self.etlspark.sqlContext.read.parquet(
            data_path_str("trusted", "busstops")
        ).filter(f"year =='{year}' and month=='{month}' and day=='{day}'")

    @staticmethod
    def save(df: DataFrame, output: str):
        (
            df.write.mode("overwrite")
            .partitionBy("year", "month", "day")
            .format("parquet")
            .save(output)
        )


class BusLineRefinedProcess:
    def __init__(self, year, month, day):
        self.etlspark = ETLSpark()
        self.bus_lines = self.filter_data(year, month, day)

    def perform(self):
        self.save(self.bus_lines, data_path_str("refined", "bus_lines"))

    def __call__(self, *args, **kwargs):
        self.perform()

    def filter_data(self, year: str, month: str, day: str) -> DataFrame:
        return self.etlspark.sqlContext.read.parquet(
            data_path_str("trusted", "lines")
        ).filter(f"year =='{year}' and month=='{month}' and day=='{day}'")

    @staticmethod
    def save(df: DataFrame, output: str):
        (
            df.write.mode("overwrite")
            .partitionBy("year", "month", "day")
            .format("parquet")
            .save(output)
        )


class BusTrackingRefinedProcess:
    def __init__(self, year, month, day):
        self.etlspark = ETLSpark()
        self.vehicles = self.filter_data(year, month, day)
        self.bus_itineraries = self.filter_bus_itineraries(year, month, day)
        self.bus_lines = self.filter_bus_lines(year, month, day)

    def perform(self):

        # Convert event_timestamp to TimestampType
        self.vehicles = self.vehicles.withColumn(
            "event_timestamp", F.to_timestamp(F.col("event_timestamp"))
        )

        # Create the dim_bus_stop DataFrame
        dim_bus_stop = self.bus_itineraries.select(
            "line_code", "latitude", "longitude", "id"
        ).distinct()

        # Rename columns to avoid ambiguity
        dim_bus_stop = dim_bus_stop.withColumnRenamed(
            "latitude", "bus_stop_latitude"
        ).withColumnRenamed("longitude", "bus_stop_longitude")

        # Join vehicles_df with dim_bus_stop_df on 'line_code'
        joined = self.vehicles.join(dim_bus_stop, on="line_code", how="left")

        # Select the desired columns from the joined DataFrame
        map_matching = joined.select(
            "line_code",
            "event_timestamp",
            "latitude",  # From vehicles
            "longitude",  # From vehicles
            "vehicle",
            "year",
            "month",
            "day",
            "id",  # From dim_bus_stop
            "bus_stop_latitude",  # From dim_bus_stop
            "bus_stop_longitude",  # From dim_bus_stop
        )

        # Calculate the haversine distance
        map_matching = map_matching.withColumn(
            "distance",
            haversine_distance_expr(
                F.col("latitude"),
                F.col("longitude"),
                F.col("bus_stop_latitude"),
                F.col("bus_stop_longitude"),
            ),
        )

        # Filter for distances <= 50 meters
        map_matching = map_matching.filter(F.col("distance") <= 50)

        # Define a window partition by line_code, vehicle and event_timestamp
        window = Window.partitionBy("line_code", "vehicle", "event_timestamp").orderBy(
            "distance"
        )

        # Find the row with minimum haversine distance within each group
        map_matching = map_matching.withColumn("row_num", F.row_number().over(window))

        # Filter for the row with row_num 1 (minimum distance)
        map_matching = map_matching.filter(F.col("row_num") == 1)

        # Calculate the mean event_timestamp for each group using the time window
        map_matching = map_matching.groupBy(
            "line_code",
            "vehicle",
            "year",
            "month",
            "day",
            "id",  # From dim_bus_stop
            F.window("event_timestamp", "10 minutes").alias("time_window"),
        ).agg(F.avg("event_timestamp").alias("mean_event_timestamp"))

        # Convert the mean_event_timestamp to TimestampType
        map_matching = map_matching.withColumn(
            "mean_event_timestamp",
            map_matching["mean_event_timestamp"].cast(T.TimestampType()),
        ).withColumnRenamed("mean_event_timestamp", "event_timestamp")

        # Select only relevant columns for perform the map matching
        map_matching = map_matching.select(
            "line_code", "vehicle", "year", "month", "day", "id", "event_timestamp"
        )

        # Select the bus line itineraries
        bus_stop_itineraries = self.bus_itineraries.select(
            "line_code", "itinerary_id", "id", "seq", "max_seq"
        )

        # Join the DataFrames on 'line_code' and 'id'
        bus_itineraries_search = map_matching.join(
            bus_stop_itineraries,
            on=["line_code", "id"],
            how="inner",  # Change to 'left', 'right', 'outer' if needed
        )

        # Define the window specification
        windowSpec = Window.partitionBy("line_code", "vehicle", "itinerary_id").orderBy(
            "event_timestamp"
        )

        # Order the DataFrame by event_timestamp within each partition
        ordered_df = bus_itineraries_search.withColumn(
            "row_num", F.row_number().over(windowSpec)
        ).orderBy("line_code", "vehicle", "itinerary_id", "row_num")

        # Create the 'next_seq_1' and 'next_seq_2' columns using lead()
        ordered_df = ordered_df.withColumn(
            "next_seq_1", F.lead(F.col("seq"), 1, None).over(windowSpec)
        ).withColumn("next_seq_2", F.lead(F.col("seq"), 2, None).over(windowSpec))

        # Filter 1 - Remove duplicate tags
        ordered_df = ordered_df.filter(
            F.col("seq") != F.col("next_seq_1")  # Condition for all points
        )

        # Filter 2 - Remove intermediate tags out of order but keep the last and second-to-last tags
        ordered_df = ordered_df.filter(
            (
                (F.col("seq") < F.col("next_seq_1"))
                & (F.col("seq") < F.col("next_seq_2"))
            )
            | (
                (F.col("seq") == F.col("max_seq"))
                | (F.col("seq") == F.col("max_seq") - 1)
            )
        )

        # Create the 'next_seq_1' and 'next_seq_2' columns using lead()
        ordered_df = ordered_df.withColumn(
            "next_seq_1", F.lead(F.col("seq"), 1, None).over(windowSpec)
        ).withColumn("next_seq_2", F.lead(F.col("seq"), 2, None).over(windowSpec))

        # Filter 3 - Remove invalid tags from the end
        filtered_df = ordered_df.filter(
            (F.col("seq") != F.col("next_seq_1"))  # Condition for all points
            & (
                (
                    (F.col("seq") < F.col("next_seq_1"))
                    & (F.col("seq") < F.col("next_seq_2"))
                )  # Condition for intermediate points
                | (
                    (F.col("seq") == F.col("max_seq")) & (F.col("next_seq_1") == 0)
                )  # Condition for the last point
                | (
                    (F.col("seq") == (F.col("max_seq") - 1))
                    & (F.col("next_seq_2") == 0)
                )
            )  # Condition for the second-to-last point
        )

        # Add the "generated" column
        filtered_df = filtered_df.withColumn("generated", F.lit(False))

        filtered_df = filtered_df.select(
            "line_code",
            "itinerary_id",
            "vehicle",
            "event_timestamp",
            "seq",
            "year",
            "month",
            "day",
            "generated",
        )

        base_df = filtered_df.select(
            "line_code",
            "itinerary_id",
            "vehicle",
            "event_timestamp",
            "seq",
            "year",
            "month",
            "day",
            "generated",
        )

        base_with_next = base_df.withColumn(
            "next_seq", F.lead(F.col("seq"), 1, None).over(windowSpec)
        ).withColumn(
            "next_event_timestamp",
            F.lead(F.col("event_timestamp"), 1, None).over(windowSpec),
        )

        gap_condition = (
            F.col("next_seq").isNotNull()
            & (F.col("next_seq") != 0)
            & (F.col("next_seq") > F.col("seq") + 1)
        )

        gaps_df = base_with_next.filter(gap_condition).withColumn(
            "generated_seq",
            F.explode(F.sequence(F.col("seq") + 1, F.col("next_seq") - 1)),
        )

        s_ts = F.unix_timestamp(F.col("event_timestamp"))
        n_ts = F.unix_timestamp(F.col("next_event_timestamp"))
        delta_seq = (F.col("next_seq") - F.col("seq")).cast("double")
        offset_seq = (F.col("generated_seq") - F.col("seq")).cast("double")

        generated_points = (
            gaps_df.withColumn(
                "interpolated_timestamp",
                F.to_timestamp(s_ts + (n_ts - s_ts) * (offset_seq / delta_seq)),
            )
            .withColumn("generated", F.lit(True))
            .filter(F.col("interpolated_timestamp") < F.col("next_event_timestamp"))
            .select(
                "line_code",
                "itinerary_id",
                "vehicle",
                F.col("interpolated_timestamp").alias("event_timestamp"),
                F.col("generated_seq").alias("seq"),
                "year",
                "month",
                "day",
                "generated",
            )
        )

        expanded_df = base_df.unionByName(generated_points)

        joined_df = expanded_df.join(
            bus_stop_itineraries,
            on=["line_code", "itinerary_id", "seq"],
            how="left",  # Change to 'inner', 'right', 'outer' if needed
        )

        # Final validation
        joined_df = joined_df.withColumn(
            "next_seq", F.lead(F.col("seq"), 1, None).over(windowSpec)
        ).withColumn("last_seq", F.lag(F.col("seq"), 1, None).over(windowSpec))

        # Apply the filter logic
        joined_df = joined_df.filter(
            (F.col("seq") == F.col("last_seq") + 1)
            | (F.col("next_seq") == F.col("seq") + 1)
            | (F.col("seq") == 0)
        )

        joined_df = joined_df.select(
            "line_code",
            "itinerary_id",
            "vehicle",
            "event_timestamp",
            "id",
            "seq",
            "year",
            "month",
            "day",
            "generated",
        )

        self.save(joined_df, data_path_str("refined", "bus_tracking"))

    def __call__(self, *args, **kwargs):
        self.perform()

    def filter_data(self, year: str, month: str, day: str) -> DataFrame:
        return self.etlspark.sqlContext.read.parquet(
            data_path_str("trusted", "vehicles")
        ).filter(f"year =='{year}' and month=='{month}' and day=='{day}'")

    def filter_bus_itineraries(self, year: str, month: str, day: str) -> DataFrame:
        return self.etlspark.sqlContext.read.parquet(
            data_path_str("refined", "bus_itineraries")
        ).filter(f"year =='{year}' and month=='{month}' and day=='{day}'")

    def filter_bus_lines(self, year: str, month: str, day: str) -> DataFrame:
        return self.etlspark.sqlContext.read.parquet(
            data_path_str("refined", "bus_lines")
        ).filter(f"year =='{year}' and month=='{month}' and day=='{day}'")

    @staticmethod
    def save(df: DataFrame, output: str):
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
