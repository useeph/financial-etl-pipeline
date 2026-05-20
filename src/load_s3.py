"""Upload partitioned parquet files from local data/raw/ to S3.

S3 layout mirrors the local one:
  s3://<bucket>/raw/stock_prices/date=YYYY-MM-DD/prices.parquet
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

LOCAL_RAW_DIR = Path("data/raw")
S3_PREFIX = "raw/stock_prices"


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )


def upload_file(local_path: Path, bucket: str, s3_key: str) -> None:
    """Upload a single file to S3."""
    s3 = get_s3_client()
    size_mb = local_path.stat().st_size / 1024 / 1024
    log.info(f"Uploading {local_path} ({size_mb:.2f} MB) -> s3://{bucket}/{s3_key}")
    s3.upload_file(str(local_path), bucket, s3_key)
    log.info("Upload complete")


def upload_partition(run_date: str) -> str:
    """Upload one date partition. Returns the S3 key written."""
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET not set in .env")

    local_path = LOCAL_RAW_DIR / f"date={run_date}" / "prices.parquet"
    if not local_path.exists():
        raise FileNotFoundError(f"No partition found at {local_path}")

    s3_key = f"{S3_PREFIX}/date={run_date}/prices.parquet"
    upload_file(local_path, bucket, s3_key)
    return s3_key


def verify_upload(run_date: str) -> dict:
    """Confirm the uploaded object exists and report its size."""
    bucket = os.getenv("S3_BUCKET")
    s3_key = f"{S3_PREFIX}/date={run_date}/prices.parquet"
    s3 = get_s3_client()
    try:
        response = s3.head_object(Bucket=bucket, Key=s3_key)
        size_mb = response["ContentLength"] / 1024 / 1024
        log.info(f"Verified: s3://{bucket}/{s3_key} ({size_mb:.2f} MB)")
        return {"key": s3_key, "size_mb": round(size_mb, 2)}
    except ClientError as e:
        log.error(f"Verification failed: {e}")
        raise


def main(run_date: str | None = None) -> None:
    # Default to the latest non-test partition
    if run_date is None:
        partitions = sorted(
            p.name.replace("date=", "")
            for p in LOCAL_RAW_DIR.glob("date=*")
            if "test" not in p.name
        )
        if not partitions:
            raise RuntimeError("No partitions found in data/raw/")
        run_date = partitions[-1]
        log.info(f"Defaulting to latest partition: {run_date}")

    upload_partition(run_date)
    verify_upload(run_date)


if __name__ == "__main__":
    main()