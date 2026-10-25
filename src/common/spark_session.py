from pyspark.sql import SparkSession
import os
from common import config


def get_spark(app_name: str) -> SparkSession:
    os.makedirs("/tmp/spark-tmp", exist_ok=True)

    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")                                   # 2 cœurs au lieu de local[*] : limite la RAM et le CPU
        .config("spark.driver.memory", "2g")                  # plafond mémoire du driver (en local, tout tourne dedans)
        .config("spark.driver.maxResultSize", "512m")         # protège contre un collect() trop gros
        .config("spark.sql.shuffle.partitions", "4")          # 200 par défaut : inutile en local
        .config("spark.sql.adaptive.enabled", "true")         # Spark ajuste les partitions à l'exécution
        .config("spark.sql.files.maxPartitionBytes", "32m")   # lecture par petits blocs
        .config("spark.sql.session.timeZone", "UTC")          # parsing des timestamps reproductible
        .config("spark.local.dir", "/tmp/spark-tmp")          # dossier des fichiers de shuffle/spill
        .config("spark.ui.showConsoleProgress", "false")      # évite la barre [Stage 1:>] qui pollue le terminal
        # --- S3A / MinIO ---
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")          # requis par MinIO
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")    # MinIO local en HTTP
        .config("spark.hadoop.fs.s3a.connection.maximum", "20")           # limite les connexions simultanées
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")         # bufferise sur disque, pas en RAM
        .getOrCreate()
    )