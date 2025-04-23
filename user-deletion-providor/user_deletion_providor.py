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

# Disable pika logging
pika_logger = logging.getLogger("pika")
pika_logger.handlers.clear()  # Removes any existing handlers
pika_logger.propagate = False
pika_logger.setLevel(logging.WARNING)  # Only show warnings and errors

open('logfile.log', 'w').close()  # Clear previous log file

# Database connection
def get_db_connection():
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"]
    )

def get_pending_deletions():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute("""
            SELECT 
                d.client_id, 
                d.deleted_at,
                c.timestamp
            FROM user_deletions_queue d
            JOIN client c ON d.client_id = c.id
            WHERE d.processed = FALSE
            ORDER BY d.deleted_at ASC
        """)
        deletions = cursor.fetchall()
        
        for deletion in deletions:
            deleted_at = deletion['deleted_at']
            if isinstance(deleted_at, str):
                deleted_at = datetime.strptime(deleted_at, '%Y-%m-%d %H:%M:%S')
            deletion['timestamp'] = deleted_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        return deletions
    except mysql.connector.Error as err:
        logger.error(f"Database error: {err}")
        return []
    finally:
        cursor.close()
        conn.close()

def mark_as_processed(client_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            UPDATE user_deletions_queue
            SET processed = TRUE
            WHERE client_id = %s
        """, (client_id,))
        conn.commit()
    except mysql.connector.Error as err:
        logger.error(f"Failed to mark deletion {client_id} as processed: {err}")
    finally:
        cursor.close()
        conn.close()

def create_xml_message(deletion):
    xml = ET.Element("UserMessage")
    
    # Action info
    ET.SubElement(xml, "ActionType").text = "DELETE"
    ET.SubElement(xml, "UUID").text = deletion['timestamp']
    ET.SubElement(xml, "TimeOfAction").text = datetime.utcnow().isoformat() + "Z"
    
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(xml, encoding='unicode')

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
            blocked_connection_timeout=300
        )
        
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # Declare exchange as TOPIC type to match existing configuration
        channel.exchange_declare(
            exchange="user",
            exchange_type="topic",
            durable=True
        )

        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)
            channel.queue_bind(
                exchange="user",
                queue=queue,
                routing_key=f"user.delete.{queue}"
            )
            channel.basic_publish(
                exchange="user",
                routing_key=f"user.delete.{queue}",
                body=xml,
                properties=pika.BasicProperties(
                    delivery_mode=2  # Make messages persistent
                )
            )
            logger.info(f"Sent XML message to {queue}")

        connection.close()
        return True
    except Exception as e:
        logger.error(f"RabbitMQ Error: {e}")
        return False

def initialize_database():
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
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    initialize_database()
    logger.info("Starting user deletion provider")
    
    while True:
        try:
            pending_deletions = get_pending_deletions()
            
            for deletion in pending_deletions:
                xml = create_xml_message(deletion)
                if send_to_rabbitmq(xml):
                    mark_as_processed(deletion['client_id'])
                    logger.info(f"Processed deletion {deletion['client_id']} with timestamp ID {deletion['timestamp']}")
                else:
                    logger.error(f"Failed to process deletion {deletion['client_id']}")
            
            time.sleep(5)
        except Exception as e:
            logger.error(f"Processing error: {e}")
            time.sleep(60)