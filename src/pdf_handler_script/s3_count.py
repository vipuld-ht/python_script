import logging
import boto3
import datetime
import os
from dotenv import load_dotenv

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")

def logging_config():
    """Configures logging to file and console."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"load_pdfs_blob_to_db_{timestamp}.log"

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.info(f"Logging started. Logs are being saved in {log_filename}")
def get_s3_files(bucket_name, prefix):
    """Lists all PDF files in the given S3 bucket and prefix, handling pagination."""
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    files = []

    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                files.extend([obj["Key"] for obj in page["Contents"] if obj["Key"].endswith(".pdf")])

        logging.info(f"Total PDFs found: {len(files)}")
        return files

    except Exception as e:
        logging.error(f"Error fetching S3 files: {e}")
        return []
    
pdf_files = get_s3_files(S3_BUCKET, S3_PREFIX)
logging.info(f"Found {len(pdf_files)} PDF files in S3.")