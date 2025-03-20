import pymysql
import logging
import uuid
import boto3
import datetime
import os
from dotenv import load_dotenv
import concurrent.futures

# Load environment variables
load_dotenv()

# Logging configuration
LOG_FILE_LOCATION = os.getenv("LOG_FILE_LOCATION")

# AWS RDS MySQL Database Credentials
DB_ENDPOINT = os.getenv("DB_ENDPOINT")
DB_PORT = int(os.getenv("DB_PORT"))
DB_NAME = os.getenv("DB_NAME")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

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


def get_db_connection():
    """Returns a database connection."""
    logging.info("Initiating database connection")
    try:
        connection = pymysql.connect(
            host=DB_ENDPOINT,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,   # Ensure commits happen automatically
            connect_timeout=240,
            read_timeout=240,
            write_timeout=240
        )
        return connection
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None


def create_table_if_not_exist():
    """Ensures the table exists in the database."""
    connection = get_db_connection()
    if connection is None:
        logging.error("Failed to establish a database connection.")
        return

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sample_pdf_data_new (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    guid VARCHAR(50) UNIQUE NOT NULL,
                    filename VARCHAR(255) NOT NULL,
                    document LONGBLOB NOT NULL,
                    uploaded_to_s3 BOOLEAN DEFAULT FALSE
                )
            """)
        connection.commit()
        logging.info("Database and table are ready.")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
    finally:
        connection.close()


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


def get_existing_filenames():
    """Fetches the list of existing PDF filenames from the database."""
    connection = get_db_connection()
    if connection is None:
        logging.error("Failed to establish a database connection.")
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


def extract_pdf_from_s3(bucket_name, key):
    """Extracts a PDF file from S3."""
    logging.info(f"Loading {key} from bucket {bucket_name}")
    s3 = boto3.client("s3")
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        logging.info(f"Successfully extracted {key} from bucket {bucket_name}")
        return key, response["Body"].read()
    except Exception as e:
        logging.error(f"Failed to extract {key} from S3: {e}")
        return None, None


def insert_pdfs(pdf_list):
    """Inserts multiple PDFs into the database in a single batch."""
    if not pdf_list:
        logging.info("No PDFs to insert.")
        return

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO sample_pdf_data_new (guid, filename, document) VALUES (%s, %s, %s)"
            logging.info(f"Executing SQL: {sql}")
            logging.info(f"Data to insert: {pdf_list}")
            cursor.executemany(sql, pdf_list)  # Batch insert
        connection.commit()
        logging.info(f"Inserted {len(pdf_list)} PDFs into the database.")
    except Exception as e:
        logging.error(f"Failed to insert batch: {e}")
    finally:
        connection.close()


def load_pdfs_from_s3(batch_size=5):
    """Loads PDFs from S3 into the database in batches after filtering existing ones."""
    pdf_files = get_s3_files(S3_BUCKET, S3_PREFIX)
    logging.info(f"Found {len(pdf_files)} PDF files in S3.")

    existing_files = get_existing_filenames()
    new_files = [pdf for pdf in pdf_files if pdf not in existing_files]

    logging.info(f"Filtered {len(existing_files)} existing PDFs. {len(new_files)} new PDFs to insert.")

    if not new_files:
        logging.info("No new PDFs to insert.")
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        for i in range(0, len(new_files), batch_size):
            pdf_files_batch = new_files[i:i+batch_size]
            logging.info(f"Processing batch {i//batch_size + 1}: {len(pdf_files_batch)} files")

            future_to_file = {executor.submit(extract_pdf_from_s3, S3_BUCKET, pdf_file): pdf_file for pdf_file in pdf_files_batch}

            pdf_list = []
            for future in concurrent.futures.as_completed(future_to_file):
                filename, file_data = future.result()
                if file_data:
                    pdf_list.append((str(uuid.uuid4()), filename, file_data))

            insert_pdfs(pdf_list)  # Batch insert


if __name__ == "__main__":
    logging_config()
    create_table_if_not_exist()
    load_pdfs_from_s3()
