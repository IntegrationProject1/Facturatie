import pika
import os
import xml.etree.ElementTree as ET
import mysql.connector
import time
import logging
from datetime import datetime

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Disable pika logging
pika_logger = logging.getLogger("pika")
pika_logger.handlers.clear()
pika_logger.propagate = False
pika_logger.setLevel(logging.WARNING)

open('logfile.log', 'w').close()

# Database connectie
def get_db_connection():
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"]
    )

# Database wachten
def wait_for_db():
    while True:
        try:
            conn = get_db_connection()
            conn.close()
            logger.info("Database connection successful")
            break
        except Exception as e:
            logger.warning(f"Waiting for database... {e}")
            time.sleep(5)

# Pending deletions ophalen
def get_pending_deletions():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT client_id, deleted_at, processed
            FROM user_deletions_queue
            ORDER BY deleted_at
            LIMIT 50
        """)
        rows = cursor.fetchall()

        # EXTRA LOGGING
        for row in rows:
            print(f"[DEBUG] Row fetched: {row}")

        # GEEN filter meer hier!!!
        return rows

    except Exception as e:
        logger.error(f"Database error: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


# Markeer als verwerkt
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
    except Exception as e:
        logger.error(f"Failed to mark deletion as processed: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# XML maken
def create_deletion_xml(deletion):
    deleted_at = deletion['deleted_at']
    if isinstance(deleted_at, str):
        deleted_at = datetime.fromisoformat(deleted_at.replace("Z", "").replace("T", " "))

    xml = ET.Element("UserMessage")
    ET.SubElement(xml, "ActionType").text = "DELETE"

    uuid_timestamp = deleted_at.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    time_of_action = deleted_at.replace(microsecond=0).isoformat() + "Z"

    ET.SubElement(xml, "UUID").text = uuid_timestamp
    ET.SubElement(xml, "TimeOfAction").text = time_of_action

    logger.debug(f"Created XML for client {deletion['client_id']}")
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(xml, encoding='unicode')

# Versturen naar RabbitMQ
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

        # Zorg dat de exchange bestaat
        channel.exchange_declare(exchange='user', exchange_type='topic', durable=True)

        # Queue aanmaken en binden
        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)
            channel.queue_bind(exchange='user', queue=queue, routing_key=f"user.delete.{queue}")

            # Publiceren naar de juiste routing key
            channel.basic_publish(
                exchange="user",
                routing_key=f"user.delete.{queue}",
                body=xml
            )
            logger.debug(f"Published deletion message to queue: {queue}")

        connection.close()
        return True
    except Exception as e:
        logger.error(f"RabbitMQ publish error: {e}")
        return False

# Database initialiseren
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

# Main loop
if __name__ == "__main__":
    wait_for_db()
    initialize_database()
    logger.info("Starting user deletion provider")
    test_deletions = get_pending_deletions()
    logger.warning(f"Test deletions fetched: {test_deletions}")

    while True:
        try:
            deletions = get_pending_deletions()

            if not deletions:
                logger.debug("No pending deletions found. Sleeping 5s...")
                time.sleep(5)
                continue

            for deletion in deletions:
                try:
                    xml = create_deletion_xml(deletion)
                    if send_to_rabbitmq(xml):
                        mark_as_processed(deletion['client_id'])
                        logger.info(f"Successfully sent deletion for client {deletion['client_id']}")
                    else:
                        logger.error(f"Failed to send deletion for client {deletion['client_id']}")
                except Exception as e:
                    logger.error(f"Error processing deletion for client {deletion['client_id']}: {e}")

            time.sleep(5)

        except Exception as e:
            logger.error(f"Main loop error: {e}")
            time.sleep(60)
