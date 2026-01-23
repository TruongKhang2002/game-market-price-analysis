import os

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

# Postgres Data Warehouse Configuration
POSTGRES_DW_USER = os.getenv("POSTGRES_DW_USER")
POSTGRES_DW_PASSWORD = os.getenv("POSTGRES_DW_PASSWORD")
POSTGRES_DW_HOST = os.getenv("POSTGRES_DW_HOST", "postgres-dw")
POSTGRES_DW_PORT = os.getenv("POSTGRES_DW_PORT", "5432")
POSTGRES_DW_DB = os.getenv("POSTGRES_DW_DB", "game_market_dw")
