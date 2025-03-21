import sys
import boto3
import logging
import time
import pymysql
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.transforms import ApplyMapping

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)

# Enable Glue bookmarks
job.init("UPLOAD_TO_S3", {"jobBookmarkOption": "job-bookmark-enable"})

# AWS RDS MySQL Database Credentials
DB_CONFIG = {
    "host": "edpnew.c7uqx6xqd0nl.us-east-1.rds.amazonaws.com",
    "port": 4897,
    "database": "policy_system",
    "user": "edpadmin",
    "password": "edpadmin",
}

# S3 Configuration
TARGET_S3_PATH = "s3://insightrag-job-config/source/glue_misc_pdf_blob_bookmarks"
BUCKET_NAME = TARGET_S3_PATH.split("/")[2]
PREFIX = "/".join(TARGET_S3_PATH.split("/")[3:])

# Initialize S3 client
s3_client = boto3.client("s3")

def fetch_pdf_records():
    """Fetches new PDF records modified since the last Glue bookmark."""
    try:
        # Load data from MySQL using Glue DynamicFrame with Bookmarking
        datasource = glueContext.create_dynamic_frame.from_options(
            connection_type="mysql",
            connection_options={
                "url": f"jdbc:mysql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
                "user": DB_CONFIG["user"],
                "password": DB_CONFIG["password"],
                "dbtable": "binder_info",
                "filterPredicate": "MODIFIED > 'bookmark:MODIFIED'"
            },
            transformation_ctx="datasource0",
        )
        applymapping1 = ApplyMapping.apply(
            frame = datasource,
            mappings = [("MODIFIED", "string", "MODIFIED", "string"),],
            transformation_ctx = "datasource"
        )
        datasink2 = glueContext.write_dynamic_frame.from_options(
            frame = applymapping1,
            connection_type = "s3",
            connection_options = {"path": "s3://insightrag-job-config/config"},
            format = "json",
            transformation_ctx = "datasink2"
        )

        record_count = datasource.count()
        logger.info(f"Fetched {record_count} new PDF records.")
        return datasource if record_count > 0 else None

    except Exception as e:
        logger.error(f"Error fetching records: {str(e)}")
        return None

def upload_to_s3(policy_id, filename, document):
    """Uploads the document to S3."""
    try:
        start_time = time.time()
        cleaned_filename = filename.replace("source/glue_misc_pdf_blob_bookmarks/", "")
        s3_key = f"{PREFIX}/{cleaned_filename}"
        
        logger.info(f"Uploading file {filename} to S3://{BUCKET_NAME}/{s3_key}...")
        
        response = s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=document)
        
        execution_time = time.time() - start_time
        logger.info(f"Uploaded {filename} in {execution_time:.2f} seconds. S3 Response: {response}")

        return True
    except Exception as e:
        logger.error(f"Failed to upload {filename}: {str(e)}")
        return False


def process_pdf_uploads():
    """Processes all PDFs: uploads them to S3."""
    dynamic_frame = fetch_pdf_records()
    
    if dynamic_frame is None:
        logger.info("No new PDFs to upload.")
        return

    df = dynamic_frame.toDF()
    logger.info(f"Converting DynamicFrame to DataFrame... Total records: {df.count()}")

    for row in df.collect():
        policy_id = row["POLICY_ID"]
        filename = f"binder_{policy_id}.pdf"
        document = row["BINDER_DOC"]  # Assuming this is binary data

        logger.info(f"Processing file for Policy ID: {policy_id}, Filename: {filename}")

        if upload_to_s3(policy_id, filename, document):
            logger.info(f"Successfully uploaded {filename} to S3.")

def main():
    """Main function to trigger the upload process."""
    logger.info("Starting the PDF upload process.")
    process_pdf_uploads()
    logger.info("PDF upload process completed.")
    
    job.commit()  # Glue bookmark saves progress
    logger.info("Glue job committed.")

if __name__ == "__main__":
    main()
