import pymysql
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# AWS RDS MySQL Database Credentials
DB_ENDPOINT = os.getenv("DB_ENDPOINT")
DB_PORT = int(os.getenv("DB_PORT"))
DB_NAME = os.getenv("DB_NAME")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Directory Configuration to Save PDFs
DOWNLOAD_DIR = os.getenv("PDF_DOWNLOAD_DIR")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def get_db_connection():
    """Returns a database connection."""
    try:
        connection = pymysql.connect(
            host=DB_ENDPOINT,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT,
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None

def fetch_pdf_by_id(record_id):
    """Fetches a single PDF from the database by ID."""
    connection = get_db_connection()
    if connection is None:
        return None

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT guid, filename, document FROM sample_pdf_data_new WHERE id = %s", (record_id,))
            pdf_record = cursor.fetchone()
        if pdf_record:
            logging.info(f"Fetched PDF with ID {record_id} from the database.")
        else:
            logging.info(f"No PDF found with ID {record_id}.")
        return pdf_record
    except Exception as e:
        logging.error(f"Failed to fetch PDF: {e}")
        return None
    finally:
        connection.close()

def save_pdf(pdf_record):
    """Saves a single PDF to the local directory."""
    if not pdf_record:
        return
    pdf_path = os.path.join(DOWNLOAD_DIR, pdf_record['filename'])
    try:
        with open(pdf_path, "wb") as pdf_file:
            pdf_file.write(pdf_record['document'])
        logging.info(f"Saved {pdf_record['filename']} to {DOWNLOAD_DIR}")
    except Exception as e:
        logging.error(f"Failed to save {pdf_record['filename']}: {e}")

if __name__ == "__main__":
    record_id = int(input("Enter the record ID to download: "))
    pdf = fetch_pdf_by_id(record_id)
    if pdf:
        save_pdf(pdf)
    else:
        logging.info("No matching PDF found.")
