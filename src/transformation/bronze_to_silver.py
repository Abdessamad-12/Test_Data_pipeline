"""Bronze -> Silver : parsing, typage, normalisation, contrôle qualité, déduplication.

Usage : python -m transformation.bronze_to_silver
"""
import logging

from pyspark.sql import DataFrame, functions as F

from common import config
from common.spark_session import get_spark

log = logging.getLogger("transformation.bronze_to_silver")

BRONZE = config.lake_path("bronze", "taxi_trips")
SILVER = config.lake_path("silver", "taxi_trips")

TEXT_COLS = ["trip_id", "taxi_id", "payment_type", "company"]
NUMERIC_TYPES = {
    "trip_seconds": "int",
    "trip_miles": "double",
    "pickup_community_area": "int",
    "dropoff_community_area": "int",
    "fare": "double",
    "tips": "double",
    "tolls": "double",
    "extras": "double",
    "trip_total": "double",
}
AMOUNTS = ["trip_miles", "fare", "tips", "tolls", "extras", "trip_total"]
MIN_TRIP_SECONDS, MAX_TRIP_SECONDS = 60, 4 * 3600
MAX_TRIP_MILES = 100
MAX_TRIP_TOTAL = 1000


def normalize_and_type(raw: DataFrame) -> DataFrame:
    return raw.select(
        # texte : trim, et chaîne vide -> NULL
        *[F.when(F.trim(F.col(c)) != "", F.trim(F.col(c))).alias(c) for c in TEXT_COLS],
        F.to_timestamp("trip_start_timestamp").alias("trip_start_timestamp"),
        F.to_timestamp("trip_end_timestamp").alias("trip_end_timestamp"),
        *[F.col(c).cast(t).alias(c) for c, t in NUMERIC_TYPES.items()],
        "year",   # colonnes de partition lues depuis le chemin bronze (year=/month=)
        "month",
    ).fillna(0, subset=["tips", "tolls", "extras"])


def is_valid() -> F.Column:
    return (
        F.col("trip_id").isNotNull()
        & F.col("trip_start_timestamp").isNotNull()
        & (F.col("trip_end_timestamp") >= F.col("trip_start_timestamp"))
        & F.col("trip_seconds").between(MIN_TRIP_SECONDS, MAX_TRIP_SECONDS)
        & F.col("trip_miles").between(0.1, MAX_TRIP_MILES)   # exclut 0 mile
        & (F.least(*[F.col(c) for c in AMOUNTS]) >= 0)
        & (F.col("trip_total") <= MAX_TRIP_TOTAL)
    )


def run() -> None:
    spark = get_spark("bronze_to_silver")

    typed = normalize_and_type(spark.read.option("header", True).csv(BRONZE)).persist()
    n_raw = typed.count()

    valid = typed.filter(is_valid())
    silver = (
        valid.dropDuplicates(["trip_id"])
        .withColumn("trip_date", F.to_date("trip_start_timestamp"))
        .persist()
    )
    n_silver = silver.count()
    n_valid = valid.count()  # lit le DataFrame typé déjà en mémoire

    silver.write.mode("overwrite").partitionBy("year", "month").parquet(SILVER)

    log.info("Bilan qualité : brut=%d | écartés (règles qualité)=%d | doublons=%d | silver=%d",
             n_raw, n_raw - n_valid, n_valid - n_silver, n_silver)

    typed.unpersist()
    silver.unpersist()
    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()