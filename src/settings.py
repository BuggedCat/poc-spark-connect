from enum import Enum
from pathlib import Path

from decouple import config


class DatalakeZones(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"

    @property
    def to_s3_uri(self):
        return f"s3a://{self.value}"


DEBUG = config("DEBUG", cast=bool, default=True)
AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID", cast=str)
AWS_SECRET_ACCESS_KEY = config("AWS_SECRET_ACCESS_KEY", cast=str)
AWS_ENDPOINT = config("AWS_ENDPOINT", cast=str)

LOCAL_DATA_DIR = Path("data")
SPARK_DATA_DIR = Path("/opt/spark/data")
