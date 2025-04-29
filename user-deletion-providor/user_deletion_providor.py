import pika
import os
import xml.etree.ElementTree as ET
import mysql.connector
import time
import logging
from datetime import datetime

# Enhanced logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('deletion_provider.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection with retries and timeouts
def get_db_connection():
    retries = 5
    delay = 5
    
    for attempt in range(retries):
        try:
            conn = mysql.connector.connect(
                host=os.getenv('DB_HOST', 'db'),  # Default to 'db' if not set
                user=os.getenv('DB_USER', 'root'),
                password=os.getenv('DB_PASSWORD', ''),
                database=os.getenv('DB_NAME', 'fossbilling'),
                port=os.getenv('DB_PORT', '3306'),
                connection_timeout=5,
                connect_timeout=5
            )
            logger.info("Successfully connected to database")
            return conn
        except mysql.connector.Error as err:
            logger.error(f"Connection attempt {attempt + 1} failed: {err}")
            if attempt == retries - 1:
                raise
            time.sleep(delay)

# Wait for database to be ready
def wait_for_db():
    logger.info("Waiting for database connection...")
    while True:
        try:
            conn = get_db_connection()
            conn.close()
            logger.info("Database connection established")
            break
        except Exception as e:
            logger.warning(f"Database not ready yet: {e}")
            time.sleep(5)

# [Keep all your other functions the same, but update mark_as_processed]
def mark_as_processed(client_id, deleted_at):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE user_deletions_queue
            SET processed = TRUE
            WHERE client_id = %s AND deleted_at = %s
        """, (client_id, deleted_at))
        conn.commit()
        logger.info(f"Marked client {client_id} as processed")
    except Exception as e:
        logger.error(f"Failed to mark as processed: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

# [Rest of your existing functions...]

# Improved main loop
if __name__ == "__main__":
    logger.info("Starting User Deletion Provider Service")
    
    while True:
        try:
            wait_for_db()
            initialize_database()
            
            while True:
                try:
                    deletions = get_pending_deletions()
                    if not deletions:
                        logger.debug("No pending deletions found")
                        time.sleep(5)
                        continue
                        
                    for deletion in deletions:
                        try:
                            logger.info(f"Processing deletion for client {deletion['client_id']}")
                            xml = create_deletion_xml(deletion)
                            if send_to_rabbitmq(xml):
                                mark_as_processed(deletion['client_id'], deletion['deleted_at'])
                            else:
                                logger.error("Failed to send to RabbitMQ")
                        except Exception as e:
                            logger.error(f"Error processing client {deletion['client_id']}: {e}")
                            
                    time.sleep(1)
                    
                except mysql.connector.Error as db_err:
                    logger.error(f"Database error in processing loop: {db_err}")
                    time.sleep(10)
                    break  # Will restart outer loop
                    
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}")
            time.sleep(30)