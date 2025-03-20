import sys
import boto3
import pymysql
import os
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# Load environment variables

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init("UPLOAD_TO_S3")

# AWS RDS MySQL Database Credentials
DB_CONFIG = {
    "host": "edpnew.c7uqx6xqd0nl.us-east-1.rds.amazonaws.com",
    "port": 4897,
    "database": "policy_system",
    "user": "edpadmin",
    "password": "edpadmin",
}

# S3 Configuration
TARGET_S3_PATH = "s3://insightrag-job-config/source/glue_misc_pdf_blob"
BUCKET_NAME = TARGET_S3_PATH.split("/")[2]
PREFIX = "/".join(TARGET_S3_PATH.split("/")[3:])

# Initialize S3 client
s3_client = boto3.client("s3")


def connect_to_database():
    """Establishes connection to the MySQL database."""
    return pymysql.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
    )


def fetch_pdf_records():
    """Fetches PDF records from MySQL database."""
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute("SELECT guid, filename, document FROM sample_pdf_data_new WHERE uploaded_to_s3 = 0")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def upload_to_s3(guid, filename, document):
    """Uploads the document to S3 and returns True if successful, else False."""
    try:
        cleaned_filename = filename.replace("source/misc_pdf_data/", "")
        s3_key = f"{PREFIX}/{cleaned_filename}"
        s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=document)
        print(f"Uploaded {filename} to S3://{BUCKET_NAME}/{s3_key}")
        return True
    except Exception as e:
        print(f"Failed to upload {filename}: {str(e)}")
        return False


def update_uploaded_status(guid):
    """Marks the file as uploaded in the database."""
    conn = connect_to_database()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE sample_pdf_data_new SET uploaded_to_s3 = 1 WHERE guid = %s", (guid,))
        conn.commit()
    except Exception as e:
        print(f"Failed to update status for {guid}: {str(e)}")
    finally:
        cursor.close()
        conn.close()


def process_pdf_uploads():
    """Processes all PDFs: uploads them to S3 and updates the database."""
    rows = fetch_pdf_records()
    for guid, filename, document in rows:
        if upload_to_s3(guid, filename, document):
            update_uploaded_status(guid)


if __name__ == "__main__":
    process_pdf_uploads()
    job.commit()
