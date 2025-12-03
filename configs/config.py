"""
Configuration for all database connections
"""

import os
from pathlib import Path
from typing import Dict

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
TEST_DIR = DATA_DIR / "test"
EXPORT_DIR = DATA_DIR / "exports"
RESULTS_DIR = BASE_DIR / "results"
SQL_DIR = BASE_DIR / "sql"

for directory in [DATA_DIR, RAW_DIR, PROCESSED_DIR, TEST_DIR, EXPORT_DIR, RESULTS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)


class DatabaseConfig:
    """Database configuration manager"""

    @staticmethod
    def postgres() -> Dict[str, str]:
        return {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "database": os.getenv("POSTGRES_DB", "dummyjson_db"),
            "user": os.getenv("POSTGRES_USER", "etl_user"),
            "password": os.getenv("POSTGRES_PASSWORD", "etl_password"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
        }

    @staticmethod
    def mongodb() -> Dict[str, any]:
        return {
            "host": os.getenv("MONGO_HOST", "localhost"),
            "port": int(os.getenv("MONGO_PORT", "27017")),
            "database": os.getenv("MONGO_DB", "dummyjson_db"),
            "user": os.getenv("MONGO_USER", "etl_user"),
            "password": os.getenv("MONGO_PASSWORD", "etl_password"),
        }

    @staticmethod
    def redis() -> Dict[str, any]:
        return {
            "host": os.getenv("REDIS_HOST", "localhost"),
            "port": int(os.getenv("REDIS_PORT", "6379")),
            "db": int(os.getenv("REDIS_DB", "0")),
        }


API_CONFIG = {
    "base_url": "https://dummyjson.com",
    "timeout": 30,
}