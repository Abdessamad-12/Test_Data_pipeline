import logging
from pathlib import Path

from pyspark.sql import DataFrame, Window, functions as F

from common import config
from common.spark_session import get_spark

log = logging.getLogger("transformation.silver_to_gold")

SILVER = config.lake_path("silver", "taxi_trips")
ZONES_CSV = Path(__file__).resolve().parents[1] / "reference" / "community_areas.csv"


def daily_revenue(trips: DataFrame) -> DataFrame:
    return trips.groupBy("trip_date").agg(
        F.count("*").alias("trips"),
        F.round(F.sum("trip_total"), 2).alias("total_revenue"),
        F.round(F.sum("tips"), 2).alias("total_tips"),
        F.round(F.avg("trip_total"), 2).alias("avg_revenue_per_trip"),
    ).orderBy("trip_date")


def daily_trip_duration(trips: DataFrame) -> DataFrame:
    return trips.groupBy("trip_date").agg(
        F.round(F.avg("trip_seconds") / 60, 2).alias("avg_duration_min"),
        F.round(F.percentile_approx("trip_seconds", 0.5) / 60, 2).alias("median_duration_min"),
        F.round(F.avg("trip_miles"), 2).alias("avg_distance_miles"),
    ).orderBy("trip_date")


def top_pickup_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    by_zone = (
        trips.filter(F.col("pickup_community_area").isNotNull())
        .groupBy("pickup_community_area")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("trip_total"), 2).alias("total_revenue"),
        )
    )
    ranked = by_zone.join(
        F.broadcast(zones), by_zone.pickup_community_area == zones.community_area, "left"
    ).select(
        F.col("pickup_community_area").alias("community_area"),
        F.coalesce("zone_name", F.lit("Unknown")).alias("zone_name"),
        "trips",
        "total_revenue",
    )
    # 77 lignes : la fenêtre globale (sans partition) est sans coût
    return ranked.withColumn("rank", F.row_number().over(Window.orderBy(F.desc("trips"))))

def top_routes(trips: DataFrame, zones: DataFrame) -> DataFrame:
    pickup = F.broadcast(zones.select(
        F.col("community_area").alias("pickup_community_area"), F.col("zone_name").alias("pickup_zone")))
    dropoff = F.broadcast(zones.select(
        F.col("community_area").alias("dropoff_community_area"), F.col("zone_name").alias("dropoff_zone")))
    return (
        trips.filter(F.col("pickup_community_area").isNotNull() & F.col("dropoff_community_area").isNotNull())
        .groupBy("pickup_community_area", "dropoff_community_area")
        .agg(F.count("*").alias("trips"), F.round(F.sum("trip_total"), 2).alias("total_revenue"))
        .join(pickup, "pickup_community_area")
        .join(dropoff, "dropoff_community_area")
        .select("pickup_zone", "dropoff_zone", "trips", "total_revenue")
        .orderBy(F.desc("trips"))
        .limit(20)
    )


def run() -> None:
    spark = get_spark("silver_to_gold")

    trips = spark.read.parquet(SILVER).cache()
    zones = spark.read.option("header", True).csv(ZONES_CSV.as_uri()).select(
        F.col("community_area").cast("int").alias("community_area"), "zone_name"
    )

    outputs = {
        "daily_revenue": daily_revenue(trips),
        "daily_trip_duration": daily_trip_duration(trips),
        "top_pickup_zones": top_pickup_zones(trips, zones),
        "top_routes": top_routes(trips, zones),
    }
    for name, df in outputs.items():
        df.coalesce(1).write.mode("overwrite").parquet(config.lake_path("gold", name))
        log.info("gold/%s écrit", name)

    log.info("Trajets sans zone de prise en charge (exclus du top) : %d",
             trips.filter(F.col("pickup_community_area").isNull()).count())

    trips.unpersist()
    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
