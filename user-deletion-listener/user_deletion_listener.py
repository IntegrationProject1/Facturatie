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
    except Exception as e:  #error handling + logging
        logger.error(f"Database error: {e}")
        return []
    finally:    #closing cursor and connection
        cursor.close()
        conn.close()

# Create XML message for RabbitMQ from user data
def create_deletion_xml(client_id):

    xml = ET.Element("UserMessage")
    ET.SubElement(xml, "ActionType").text = "DELETE"
    ET.SubElement(xml, "UserID").text = str(client_id)
    ET.SubElement(xml, "TimeOfAction").text = datetime.utcnow().isoformat() + "Z"
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(xml, encoding='unicode')    #returning xml message

# Send XML message to RabbitMQ exchange for user deletion
def send_to_rabbitmq(xml):

    try:
        params = pika.ConnectionParameters(
            host=os.environ["RABBITMQ_HOST"],
            port=int(os.environ["RABBITMQ_PORT"]),
            virtual_host="/",
            credentials=pika.PlainCredentials(
                os.environ["RABBITMQ_USER"],
                os.environ["RABBITMQ_PASSWORD"]
            )
        )
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue="facturatie_user_delete", durable=True)
        channel.basic_publish(
            exchange="user",
            routing_key="facturatie.user.delete",
            body=xml    #sending xml message to RabbitMQ from above
        )
        connection.close()
        return True
    except Exception as e:  #error handling + logging
        logger.error(f"RabbitMQ Error: {e}")
        return False

# Mark user deletion as processed
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
    except Exception as e:  #error handling + logging
        logger.error(f"Failed to mark deletion processed: {e}")
    finally:    #closing cursor and connection
        cursor.close()
        conn.close()

# Main loop
def main():
    logger.info("Starting user deletion listener")
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
            time.sleep(5)
        except Exception as e:
            logger.error(f"Processing error: {e}")
            time.sleep(60)