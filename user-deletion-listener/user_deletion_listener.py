import pika
import os
import xml.etree.ElementTree as ET
from datetime import datetime
import mysql.connector
import time
import logging

# For logging and debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection
def get_db_connection():
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"]
    )

# Get pending deletions from database that have not been processed yet
def get_pending_deletions():

    # Establish connection
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute("""
            SELECT client_id, deleted_at 
            FROM user_deletions_queue
            WHERE processed = FALSE
            ORDER BY deleted_at
            LIMIT 50
        """)
        return cursor.fetchall()
    except mysql.connector.Error as err:        #error handling + logging
        logger.error(f"Database error: {err}")
        return []
    finally:               #closing cursor and connection
        cursor.close()
        conn.close()

# Mark user deletion as processed
def mark_as_processed(client_id):

    # Establish connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            UPDATE user_deletions_queue
            SET processed = TRUE
            WHERE client_id = %s
        """, (client_id,))
        conn.commit()
    except mysql.connector.Error as err:    #error handling + logging
        logger.error(f"Failed to mark deletion processed for user {client_id}: {err}")
    finally:           #closing cursor and connection
        cursor.close()
        conn.close()

# Create XML message for RabbitMQ from user data
def create_deletion_xml(client_id):

    try:
        email = get_email_by_id(client_id)
        if not email:
            logger.warning(f"Client {client_id} exists but has no email - using fallback")
            email = f"unknown_{client_id}@placeholder.com"

        xml = ET.Element("UserMessage")
        ET.SubElement(xml, "ActionType").text = "DELETE"
        
        email_elem = ET.SubElement(xml, "Email")
        email_elem.text = email
        
        ET.SubElement(xml, "OriginalClientID").text = str(client_id)
        ET.SubElement(xml, "TimeOfAction").text = datetime.utcnow().isoformat() + "Z"
        
        return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(xml, encoding='unicode')
    except Exception as e:
        logger.error(f"XML generation failed for client {client_id}: {e}")
        return None

def get_email_by_id(client_id):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT email FROM client 
            WHERE id = %s 
            AND email IS NOT NULL 
            AND email != ''
        """, (client_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Email lookup error for client {client_id}: {e}")
        return None
    finally:
        if conn:
            conn.close()

# Send XML message to RabbitMQ exchange for user deletion
def send_to_rabbitmq(xml):

    # Establish connection to RabbitMQ
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
            blocked_connection_timeout=300
        )
        
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        
        channel.queue_declare(queue="facturatie_user_delete", durable=True)
        channel.basic_publish(
            exchange="user",
            routing_key="facturatie.user.delete",
            body=xml    #sending xml message to RabbitMQ
        )
        
        connection.close()
        return True
    except Exception as e:  #error handling + logging
        logger.error(f"RabbitMQ Error: {e}")
        return False

# Initialize database for safety and to avoid errors
def initialize_database():

    # Establish connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_deletions_queue (
                id INT AUTO_INCREMENT PRIMARY KEY,
                client_id INT NOT NULL UNIQUE,
                deleted_at DATETIME NOT NULL,
                processed BOOLEAN DEFAULT FALSE
            )
        """)
        conn.commit()
    except Exception as e:  #error handling + logging
        logger.error(f"Database initialization failed: {e}")
    finally:       #closing cursor and connection
        cursor.close()
        conn.close()

# Main loop
# Get pending deletions every 5 seconds, create XML message and send to RabbitMQ
if __name__ == "__main__":
    initialize_database()
    logger.info("Starting user deletion listener")  #logging
    
    while True:
        try:
            deletions = get_pending_deletions()
            
            for deletion in deletions:
                xml = create_deletion_xml(deletion['client_id'])
                if send_to_rabbitmq(xml):
                    mark_as_processed(deletion['client_id'])
                    logger.info(f"Processed deletion for user {deletion['client_id']}")
                else:
                    logger.error(f"Failed to process deletion for user {deletion['client_id']}")
            
            time.sleep(5)   # every 5 seconds the loop will run again to check for pending deletions
        except Exception as e: #error handling + logging
            logger.error(f"Processing error: {e}")
            time.sleep(60)  # if there is an error, the loop will wait for 60 seconds before running again