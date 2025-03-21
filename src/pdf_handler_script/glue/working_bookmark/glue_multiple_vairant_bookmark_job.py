import sys
import hashlib
import logging
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import concat_ws

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)

# Configure bookmark strategy (change this to switch modes)
BOOKMARK_STRATEGY = "timestamp"  # Options: "id", "timestamp", "composite"

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

def configure_bookmark_strategy():
    """Configure bookmark parameters based on selected strategy"""
    strategies = {
        "id": {
            "keys": ["ID"],
            "sort_order": "asc",
            "query": f"SELECT * FROM {DB_CONFIG['table']} ORDER BY ID"
        },
        "timestamp": {
            "keys": ["MODIFIED"],
            "sort_order": "asc",
            "query": f"SELECT * FROM {DB_CONFIG['table']} ORDER BY MODIFIED"
        },
        "composite": {
            "keys": ["ID", "POLICY_ID", "COMPANY", "BROKER", "MODIFIED"],
            "sort_order": "asc",
            "query": f"""
                SELECT *, 
                SHA1(CONCAT(
                    COALESCE(ID,''), 
                    COALESCE(POLICY_ID,''), 
                    COALESCE(COMPANY,''), 
                    COALESCE(BROKER,''), 
                    COALESCE(MODIFIED,'')
                )) as RECORD_HASH 
                FROM {DB_CONFIG['table']}
            """
        }
    }
    
    if BOOKMARK_STRATEGY not in strategies:
        raise ValueError(f"Invalid bookmark strategy. Choose from {list(strategies.keys())}")
    
    return strategies[BOOKMARK_STRATEGY]


def fetch_records():
    """Fetch records using configured bookmark strategy"""
    strategy_config = configure_bookmark_strategy()
    
    connection_options = {
        "url": f"jdbc:mysql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"],
        "dbtable": DB_CONFIG['table'],  # Specify the table name here
        "jobBookmarkKeys": strategy_config["keys"],
        "jobBookmarkKeysSortOrder": strategy_config["sort_order"],
        "hashfield": "MODIFIED" if BOOKMARK_STRATEGY == "timestamp" else None
    }

    logger.info(f"Using bookmark strategy: {BOOKMARK_STRATEGY}")
    logger.info(f"Bookmark keys: {strategy_config['keys']}")

    return glueContext.create_dynamic_frame.from_options(
        connection_type="mysql",
        connection_options=connection_options,
        transformation_ctx="jdbc_connection"
    )

def upload_to_s3(dyf):
    """Upload the processed DynamicFrame to S3"""
    try:
        glueContext.write_dynamic_frame.from_options(
            frame=dyf,
            connection_type="s3",
            connection_options={
                "path": TARGET_S3_PATH,  # Your S3 target path
                "partitionKeys": []  # Specify partition keys if needed
            },
            format="json"  # Change to your desired format (e.g., "parquet")
        )
        logger.info(f"Successfully uploaded data to {TARGET_S3_PATH}")
    except Exception as e:
        logger.error(f"Error uploading data to S3: {str(e)}")


def process_records(dyf):
    """Process records with hash verification"""
    if dyf.count() == 0:
        logger.info("No new records to process")
        return

    df = dyf.toDF()
    
    if BOOKMARK_STRATEGY == "composite":
        # Add hash column for verification
        df = df.withColumn(
            "CALCULATED_HASH",
            concat_ws("|", *[col.lower() for col in configure_bookmark_strategy()["keys"]])
        )
    
    # Process records
    processed_count = 0
    for row in df.collect():
        try:
            policy_id = row["POLICY_ID"]
            logger.info(f"Processing Policy ID: {policy_id}")
            
            # Add custom processing logic here
            processed_count += 1
            
        except Exception as e:
            logger.error(f"Error processing record: {str(e)}")

    logger.info(f"Successfully processed {processed_count}/{dyf.count()} records")
    
    # Convert back to DynamicFrame for uploading
    processed_dyf = glueContext.create_dynamic_frame.fromDF(df, glueContext, "processed_dyf")
    
    # Upload the processed DynamicFrame to S3
    upload_to_s3(processed_dyf)
    logger.info(f"Loading processed {processed_count}/{dyf.count()} records")


def main():
    """Main job execution"""
    try:
        job.init("UPLOAD_TO_S3", {"jobBookmarkOption": "job-bookmark-enable"})
        dyf = fetch_records()
        process_records(dyf)
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    finally:
        job.commit()
        logger.info("Job committed with updated bookmarks")

if __name__ == "__main__":
    main()
