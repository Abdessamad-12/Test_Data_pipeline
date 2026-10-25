from urllib.parse import urlparse
from minio import Minio
from common import config


def get_minio_client() -> Minio:
    url = urlparse(config.MINIO_ENDPOINT)
    return Minio(
        url.netloc,
        access_key=config.MINIO_USER,
        secret_key=config.MINIO_PASSWORD,
        secure=url.scheme == "https",
    )


def ensure_bucket(client: Minio) -> None:
    """Crée le bucket s'il n'existe pas (idempotent)."""
    if not client.bucket_exists(config.BUCKET):
        client.make_bucket(config.BUCKET)