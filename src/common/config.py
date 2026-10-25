import os

from dotenv import load_dotenv

load_dotenv()

# Période ingérée (filtre sur trip_start_timestamp) : un trimestre
START_MONTH = "2023-01"
END_MONTH = "2023-03"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.environ["MINIO_ROOT_USER"]
MINIO_PASSWORD = os.environ["MINIO_ROOT_PASSWORD"]
SODA_APP_TOKEN = os.getenv("SODA_APP_TOKEN") #Optionnel

BUCKET = "datalake"


def lake_path(layer: str, *parts: str) -> str:
    """Chemin Spark d'une couche du datalake : s3a://datalake/<layer>/<parts>"""
    return "/".join([f"s3a://{BUCKET}", layer, *parts])