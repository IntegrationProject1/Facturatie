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

# Create XML
def create_deletion_xml(deletion):
    deleted_at = deletion['deleted_at']
    if isinstance(deleted_at, str):
        # Handle both string and datetime objects
        try:
            deleted_at = datetime.strptime(deleted_at, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                deleted_at = datetime.fromisoformat(deleted_at.replace("Z", "").replace("T", " "))
            except ValueError as e:
                logger.error(f"Could not parse deleted_at: {deleted_at} - {e}")
                raise

    xml = ET.Element("UserMessage")
    ET.SubElement(xml, "ActionType").text = "DELETE"

    uuid_timestamp = deleted_at.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    time_of_action = deleted_at.replace(microsecond=0).isoformat() + "Z"

    ET.SubElement(xml, "UUID").text = uuid_timestamp
    ET.SubElement(xml, "TimeOfAction").text = time_of_action

    logger.debug(f"Created XML for client {deletion['client_id']}")
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(xml, encoding='unicode')

# Send to RabbitMQ
def send_to_rabbitmq(xml):
    queues = ["crm_user_delete", "kassa_user_delete", "frontend_user_delete"]

    try:
        params = pika.ConnectionParameters(
            host=os.environ["RABBITMQ_HOST"],
            port=int(os.environ["RABBITMQ_PORT"]),
            virtual_host="/",
            credentials=pika.PlainCredentials(
                os.environ["RABBITMQ_USER"],
                os.environ["RABBITMQ_PASSWORD"]
            ),
            heartbeat=600,
            blocked_connection_timeout=300,
            socket_timeout=5  # Added socket timeout
        )

        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)
            channel.basic_publish(
                exchange="user",
                routing_key=f"user.delete.{queue}",
                body=xml
            )
            logger.info(f"Published deletion message to queue: {queue}")

        connection.close()
        return True
    except Exception as e:
        logger.error(f"RabbitMQ publish error: {e}")
        return False

# Database initialization
def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_deletions_queue (
                id INT AUTO_INCREMENT PRIMARY KEY,
                client_id INT NOT NULL,
                deleted_at DATETIME NOT NULL,
                processed BOOLEAN DEFAULT FALSE,
                UNIQUE KEY unique_deletion (client_id, deleted_at)
            )
        """)
        conn.commit()
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
    finally:
        cursor.close()
        conn.close()

# Main loop
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