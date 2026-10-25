"""Ingestion bronze : Chicago Taxi Trips (API SODA) -> MinIO bronze/taxi_trips/

Usage : python -m ingestion.api_soda --start 2023-01 --end 2023-03
"""
import argparse
import io
import logging
from datetime import date

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common import config
from common.storage import ensure_bucket, get_minio_client

SODA_URL = "https://data.cityofchicago.org/resource/wrvz-psew.csv"
PAGE_SIZE = 50_000
PREFIX = "bronze/taxi_trips"

log = logging.getLogger("ingestion.api_soda")


def month_bounds(start: str, end: str):
    """Génère (premier jour, premier jour du mois suivant) pour chaque mois, de start à end inclus."""
    y, m = map(int, start.split("-"))
    ye, me = map(int, end.split("-"))
    while (y, m) <= (ye, me):
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        yield date(y, m, 1), date(ny, nm, 1)
        y, m = ny, nm


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    if config.SODA_APP_TOKEN:
        session.headers["X-App-Token"] = config.SODA_APP_TOKEN
    return session


def ingest_month(session, minio, first: date, nxt: date) -> int:
    where = f"trip_start_timestamp >= '{first}T00:00:00' AND trip_start_timestamp < '{nxt}T00:00:00'"
    total, page = 0, 0
    while True:
        resp = session.get(
            SODA_URL,
            params={"$where": where, "$order": ":id", "$limit": PAGE_SIZE, "$offset": page * PAGE_SIZE},
            timeout=120,
        )
        resp.raise_for_status()
        rows = resp.content.count(b"\n") - 1  # moins l'en-tête
        if rows <= 0:
            break
        key = f"{PREFIX}/year={first.year}/month={first.month:02d}/part-{page:05d}.csv"
        minio.put_object(config.BUCKET, key, io.BytesIO(resp.content), length=len(resp.content), content_type="text/csv")
        total += rows
        page += 1
        log.info("%s page %d : %d lignes (cumul %d)", first.strftime("%Y-%m"), page, rows, total)
        if rows < PAGE_SIZE:
            break
    return total


def run(start: str, end: str) -> int:
    minio = get_minio_client()
    ensure_bucket(minio)
    session = make_session()
    return sum(ingest_month(session, minio, a, b) for a, b in month_bounds(start, end))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=config.START_MONTH, help="YYYY-MM")
    parser.add_argument("--end", default=config.END_MONTH, help="YYYY-MM (inclus)")
    args = parser.parse_args()
    log.info("Total ingéré : %d lignes", run(args.start, args.end))