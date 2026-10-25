"""Couche d'exposition SQL : DuckDB lit les tables gold (Parquet) directement dans MinIO.

Usage : python -m serving.sql_engine "SELECT * FROM daily_revenue LIMIT 5"
"""
import sys
from urllib.parse import urlparse

import duckdb

from common import config

GOLD_TABLES = ["daily_revenue", "daily_trip_duration", "top_pickup_zones", "top_routes"]


def get_connection() -> duckdb.DuckDBPyConnection:
    url = urlparse(config.MINIO_ENDPOINT)
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")  # lecture de fichiers distants (MinIO)
    con.execute(f"""
        CREATE SECRET minio (
            TYPE S3,
            KEY_ID '{config.MINIO_USER}',
            SECRET '{config.MINIO_PASSWORD}',
            ENDPOINT '{url.netloc}',
            URL_STYLE 'path',
            USE_SSL {str(url.scheme == "https").lower()}
        )
    """)
    for table in GOLD_TABLES:
        con.execute(
            f"CREATE VIEW {table} AS "
            f"SELECT * FROM read_parquet('s3://{config.BUCKET}/gold/{table}/*.parquet')"
        )
    return con


if __name__ == "__main__":
    get_connection().sql(sys.argv[1]).show(max_width=200)