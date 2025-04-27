import pika
import os
import logging
import xml.etree.ElementTree as ET
import mysql.connector
import time

# Logging setup
logging.basicConfig(
    level=logging.INFO,
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
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

# Lookup client_id via UUID timestamp
def find_client_id_by_uuid(uuid_timestamp):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Strip microseconden voor vergelijking
        base_timestamp = uuid_timestamp.split('.')[0]  # Neem enkel "2025-04-26T14:53:12"

        cursor.execute("""
            SELECT client_id 
            FROM user_deletions_queue
            WHERE DATE_FORMAT(deleted_at, '%Y-%m-%dT%H:%i:%s') = %s
              AND processed = FALSE
        """, (base_timestamp,))
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            return None
    except Exception as e:
        logger.error(f"Lookup failed: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


# Delete client by ID
def delete_client_by_id(client_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM client WHERE id = %s", (client_id,))
        conn.commit()
        logger.info(f"Deleted client with ID: {client_id}")
    except Exception as e:
        logger.error(f"Deletion failed: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Mark deletion as processed
def mark_deletion_as_processed(client_id):
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
        logger.error(f"Failed to mark deletion processed: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Verwerken van ontvangen berichten
def on_message(channel, method, properties, body):
    try:
        xml_data = body.decode()
        root = ET.fromstring(xml_data)
        uuid_timestamp = root.find('UUID').text

        logger.info(f"Received delete request for UUID: {uuid_timestamp}")

        client_id = find_client_id_by_uuid(uuid_timestamp)
        if client_id:
            delete_client_by_id(client_id)
            mark_deletion_as_processed(client_id)
            logger.info(f"Successfully processed deletion for client ID: {client_id}")
        else:
            logger.warning(f"No matching client found for UUID: {uuid_timestamp}")

        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error(f"Message processing failed: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

# Consumer starten
def start_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST"),
        port=int(os.getenv("RABBITMQ_PORT")),
        credentials=pika.PlainCredentials(
            os.getenv("RABBITMQ_USER"),
            os.getenv("RABBITMQ_PASSWORD")
        )
    ))
    channel = connection.channel()

    try:
        queue_name = 'facturatie_user_delete'

        # Belangrijk: declare queue en bind!
        channel.exchange_declare(exchange='user', exchange_type='topic', durable=True)
        channel.queue_declare(queue=queue_name, durable=True)
        channel.queue_bind(exchange='user', queue=queue_name, routing_key='user.delete.facturatie_user_delete')

        logger.info("Waiting 2 seconds before consuming...")
        time.sleep(2)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=on_message,
            auto_ack=False
        )

        logger.info(f"Waiting for user deletion messages on queue {queue_name}...")
        channel.start_consuming()

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
        channel.stop_consuming()
        connection.close()
        logger.info("Consumer stopped.")

    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        raise

if __name__ == "__main__":
    start_consumer()
