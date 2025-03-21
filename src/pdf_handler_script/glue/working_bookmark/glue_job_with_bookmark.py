
import sys
import boto3
import logging
import time
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)

# Initialize with bookmarks
job.init("UPLOAD_TO_S3", {"jobBookmarkOption": "job-bookmark-enable"})

# Database configuration
DB_CONFIG = {
    "host": "edpnew.c7uqx6xqd0nl.us-east-1.rds.amazonaws.com",
    "port": 4897,
    "database": "policy_system",
    "user": "edpadmin",
    "password": "edpadmin",
    "table": "binder_info"
}

# S3 Configuration
TARGET_S3_PATH = "s3://insightrag-job-config/source/glue_misc_pdf_blob_bookmarks"
BUCKET_NAME = TARGET_S3_PATH.split("/")[2]
PREFIX = "/".join(TARGET_S3_PATH.split("/")[3:])

def fetch_incremental_records():
    """Fetch only new/modified records using Glue bookmarks"""
    try:
        connection_options = {
            "url": f"jdbc:mysql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
            "dbtable": DB_CONFIG["table"],
            "user": DB_CONFIG["user"],
            "password": DB_CONFIG["password"],
            "jobBookmarkKeys": ["ID", "POLICY_ID", "COMPANY", "BROKER", "MODIFIED"],  # Composite key for safety
            "jobBookmarkKeysSortOrder": "asc",
            "hashfield": "MODIFIED"  # Ensures ordering by modification time
        }

        return glueContext.create_dynamic_frame.from_options(
            connection_type="mysql",
            connection_options=connection_options,
            transformation_ctx="jdbc_connection"
        )
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

def upload_to_s3(policy_id, document):
    """Efficient S3 upload with checks"""
    try:
        s3_key = f"{PREFIX}/binder_{policy_id}.pdf"
        s3 = boto3.resource('s3')
        obj = s3.Object(BUCKET_NAME, s3_key)
        
        # Skip upload if file exists and is same size
        if 'Contents' in s3.meta.client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=s3_key):
            existing_size = s3.meta.client.head_object(Bucket=BUCKET_NAME, Key=s3_key)['ContentLength']
            if len(document) == existing_size:
                logger.info(f"Skipping existing file {s3_key}")
                return False
                
        obj.put(Body=bytes(document), Metadata={"policy_id": str(policy_id)})
        return True
    except Exception as e:
        logger.error(f"Upload failed for {policy_id}: {str(e)}")
        return False

def process_records():
    """Main processing logic with bookmark awareness"""
    dyf = fetch_incremental_records()
    
    if dyf.count() == 0:
        logger.info("No new records to process")
        return

    # Convert to Spark DataFrame
    df = dyf.toDF().persist()
    logger.info(f"Processing {df.count()} new/changed records")

    # Efficient processing with batch operations
    success_count = df.rdd.map(lambda row: (
        row["POLICY_ID"],
        upload_to_s3(row["POLICY_ID"], row["BINDER_DOC"])
    )).filter(lambda x: x[1]).count()

    logger.info(f"Successfully processed {success_count}/{df.count()} records")

def main():
    """Main entry point with error handling"""
    try:
        logger.info("Starting incremental upload job")
        process_records()
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    finally:
        job.commit()
        logger.info("Job committed with updated bookmarks")

if __name__ == "__main__":
    main()
