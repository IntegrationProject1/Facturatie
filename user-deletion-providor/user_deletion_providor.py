import pika
import os
import xml.etree.ElementTree as ET
from datetime import datetime
import mysql.connector
import time
import logging

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

open('logfile.log', 'w').close()  # Clear previous log file

# Database connection
def get_db_connection():
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"]
    )

# Haal alle users die nog niet verwerkt zijn (processed = 0)
def get_users_to_delete():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT 
                client_id, 
                deleted_at
            FROM user_deletion_notifications
            WHERE processed = 0
            ORDER BY deleted_at ASC
        """)
        users = cursor.fetchall()

        for user in users:
            deleted_at = user['deleted_at']
            if isinstance(deleted_at, str):
                deleted_at = datetime.strptime(deleted_at, '%Y-%m-%d %H:%M:%S.%f')
            user['timestamp'] = deleted_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        return users
    except mysql.connector.Error as err:
        logger.error(f"Database error: {err}")
        return []
    finally:
        cursor.close()
        conn.close()

# Zet processed = 1 zodat hij niet opnieuw verzonden wordt
def mark_as_deleted(client_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            UPDATE user_deletion_notifications
            SET processed = 1
            WHERE client_id = %s
        """, (client_id,))
        conn.commit()
    except mysql.connector.Error as err:
        logger.error(f"Failed to mark client {client_id} as processed: {err}")
    finally:
        cursor.close()
        conn.close()

# Maak een geldig XML-bestand volgens het XSD-formaat (DELETE)
def create_delete_xml(user):
    xml = ET.Element("UserMessage")

    ET.SubElement(xml, "ActionType").text = "DELETE"
    ET.SubElement(xml, "UUID").text = user['timestamp']
    ET.SubElement(xml, "TimeOfAction").text = datetime.utcnow().isoformat() + "Z"

    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(xml, encoding='unicode')

# Verstuur XML naar RabbitMQ queues
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
                    delivery_mode=2
                )
            )
            logger.info(f"Sent DELETE XML message to {queue}")

        connection.close()
        return True
    except Exception as e:
        logger.error(f"RabbitMQ Error: {e}")
        return False

# Optioneel: initialiseer tabel als die nog niet bestaat
def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_deletion_notifications (
                id INT AUTO_INCREMENT PRIMARY KEY,
                client_id INT NOT NULL,
                deleted_at DATETIME(6) NOT NULL,
                timestamp DATETIME(6),
                processed BOOLEAN DEFAULT 0
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
    initialize_database()
    logger.info("Starting user deletion provider")

    while True:
        try:
            users_to_delete = get_users_to_delete()

            for user in users_to_delete:
                xml = create_delete_xml(user)
                if send_to_rabbitmq(xml):
                    mark_as_deleted(user['client_id'])
                    logger.info(f"Processed deletion for client {user['client_id']} with timestamp {user['timestamp']}")
                else:
                    logger.error(f"Failed to process deletion for client {user['client_id']}")

            time.sleep(5)
        except Exception as e:
            logger.error(f"Processing error: {e}")
            time.sleep(60)
