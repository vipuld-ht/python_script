import pymysql
import logging
import boto3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# AWS RDS MySQL Database Credentials
DB_ENDPOINT = os.getenv("DB_ENDPOINT")
DB_PORT = int(os.getenv("DB_PORT"))
DB_NAME = os.getenv("DB_NAME")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")

def get_db_connection():
    """Returns a database connection."""
    try:
        connection = pymysql.connect(
            host=DB_ENDPOINT,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )
        return connection
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None

def get_s3_files(bucket_name, prefix):
    """Lists all PDF files in the given S3 bucket and prefix."""
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    files = set()

    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                files.update(obj["Key"] for obj in page["Contents"] if obj["Key"].endswith(".pdf"))
        logging.info(f"Total PDFs in S3: {len(files)}")
        return files
    except Exception as e:
        logging.error(f"Error fetching S3 files: {e}")
        return set()

def get_existing_filenames():
    """Fetches the list of existing PDF filenames from the database."""
    connection = get_db_connection()
    if connection is None:
        return set()

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT filename FROM sample_pdf_data_new")
            existing_files = {row["filename"] for row in cursor.fetchall()}
        logging.info(f"Fetched {len(existing_files)} existing filenames from DB.")
        return existing_files
    except Exception as e:
        logging.error(f"Error fetching existing filenames: {e}")
        return set()
    finally:
        connection.close()

def remove_extra_files(extra_files):
    """Removes extra records from the database that are not in S3."""
    if not extra_files:
        logging.info("No extra files found in DB to delete.")
        return

    connection = get_db_connection()
    if connection is None:
        return

    try:
        with connection.cursor() as cursor:
            sql = "DELETE FROM sample_pdf_data_new WHERE filename IN (%s)"
            formatted_sql = sql % ", ".join(["%s"] * len(extra_files))
            cursor.execute(formatted_sql, tuple(extra_files))
        connection.commit()
        logging.info(f"Deleted {len(extra_files)} extra records from the database.")
    except Exception as e:
        logging.error(f"Failed to delete extra records: {e}")
    finally:
        connection.close()

def sync_db_with_s3():
    """Compares S3 and DB records and removes extra records from DB."""
    s3_files = get_s3_files(S3_BUCKET, S3_PREFIX)
    db_files = get_existing_filenames()
    
    extra_files = db_files - s3_files
    logging.info(f"Extra files in DB to be removed: {extra_files}")
    remove_extra_files(extra_files)

if __name__ == "__main__":
    sync_db_with_s3()
