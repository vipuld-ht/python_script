import sys
import boto3
import pymysql
import logging
import time
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init("UPLOAD_TO_S3")

# AWS RDS MySQL Database Credentials
DB_CONFIG = {
    host": "your_db_host",
    "port": 4897,
    "database": "your_db",
    "user": "your_username",
    "password": "your_password",
}

# S3 Configuration
TARGET_S3_PATH = "s3://insightrag-job-config/source/glue_misc_pdf_blob"
BUCKET_NAME = TARGET_S3_PATH.split("/")[2]
PREFIX = "/".join(TARGET_S3_PATH.split("/")[3:])

# Initialize S3 client
s3_client = boto3.client("s3")

def connect_to_database():
    """Establishes connection to the MySQL database."""
    try:
        conn = pymysql.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
        )
        logger.info("Successfully connected to the database.")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        sys.exit(1)

def fetch_pdf_records():
    """Fetches PDF records from MySQL database."""
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        cursor.execute("SELECT guid, filename, document FROM sample_pdf_data_new WHERE uploaded_to_s3 = 0")
        rows = cursor.fetchall()
        logger.info(f"Fetched {len(rows)} records from database.")
        return rows
    except Exception as e:
        logger.error(f"Error fetching records: {str(e)}")
        return []
    finally:
        cursor.close()
        conn.close()

def upload_to_s3(guid, filename, document):
    """Uploads the document to S3 and returns True if successful, else False."""
    try:
        start_time = time.time()
        cleaned_filename = filename.replace("source/misc_pdf_data/", "")
        s3_key = f"{PREFIX}/{cleaned_filename}"
        s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=document)
        execution_time = time.time() - start_time
        logger.info(f"Uploaded {filename} to S3://{BUCKET_NAME}/{s3_key} in {execution_time:.2f} seconds")
        return True
    except Exception as e:
        logger.error(f"Failed to upload {filename}: {str(e)}")
        return False

def update_uploaded_status(guid):
    """Marks the file as uploaded in the database."""
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        cursor.execute("UPDATE sample_pdf_data_new SET uploaded_to_s3 = 1 WHERE guid = %s", (guid,))
        conn.commit()
        logger.info(f"Updated database status for {guid}.")
    except Exception as e:
        logger.error(f"Failed to update status for {guid}: {str(e)}")
    finally:
        cursor.close()
        conn.close()

def process_pdf_uploads():
    """Processes all PDFs: uploads them to S3 and updates the database."""
    rows = fetch_pdf_records()
    if not rows:
        logger.info("No new PDFs to upload.")
        return
    
    for guid, filename, document in rows:
        if upload_to_s3(guid, filename, document):
            logger.info("Successfully uploaded the {filename}.")
            

def main():
    """Main function to trigger the upload process."""
    logger.info("Starting the PDF upload process.")
    process_pdf_uploads()
    logger.info("PDF upload process completed.")
    job.commit()

if __name__ == "__main__":
    main()
