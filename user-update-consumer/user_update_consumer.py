import pika
import os
import logging
import xml.etree.ElementTree as ET
import mysql.connector
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def user_exists(uuid_timestamp):
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM client WHERE timestamp = %s", (uuid_timestamp,))
        return cursor.fetchone() is not None
    finally:
        cursor.close()
        conn.close()

def update_user(user_data):
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor()
    try:
        uuid_timestamp = user_data['uuid']
        if not user_exists(uuid_timestamp):
            logger.warning(f"Client with timestamp {uuid_timestamp} not found - nothing to update")
            return False

        update_fields = []
        update_values = []

        mappings = {
            'EncryptedPassword': 'pass',
            'FirstName': 'first_name',
            'LastName': 'last_name',
            'PhoneNumber': 'phone',
            'EmailAddress': 'email',
            'BusinessName': 'company',
            'BusinessEmail': 'company_vat',
            'RealAddress': 'address_1',
            'BTWNumber': 'company_number',
            'FacturationAddress': 'address_2'
        }

        for key, db_field in mappings.items():
            if key in user_data:
                update_fields.append(f"{db_field} = %s")
                update_values.append(user_data[key])

        if not update_fields:
            logger.info("No update fields provided.")
            return False

        update_values.append(uuid_timestamp)
        query = f"UPDATE client SET {', '.join(update_fields)}, updated_at = NOW() WHERE timestamp = %s"
        cursor.execute(query, tuple(update_values))
        conn.commit()
        logger.info(f"Updated client with timestamp: {uuid_timestamp}")
        return True
    except Exception as e:
        logger.error(f"Update failed: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def parse_user_xml(xml_data):
    try:
        root = ET.fromstring(xml_data)
        user_data = {
            'action_type': root.find('ActionType').text,
            'uuid': root.find('UUID').text,
            'action_time': root.find('TimeOfAction').text
        }

        for tag in ['EncryptedPassword', 'FirstName', 'LastName', 'PhoneNumber', 'EmailAddress']:
            el = root.find(tag)
            if el is not None:
                user_data[tag] = el.text

        business = root.find('Business')
        if business is not None:
            for tag in ['BusinessName', 'BusinessEmail', 'RealAddress', 'BTWNumber', 'FacturationAddress']:
                el = business.find(tag)
                if el is not None:
                    user_data[tag] = el.text

        return user_data
    except Exception as e:
        logger.error(f"XML parsing failed: {e}")
        raise

def on_message(channel, method, properties, body):
    try:
        logger.info(f"Received message from {method.routing_key}")
        user_data = parse_user_xml(body.decode())

        if user_data['action_type'].upper() != 'UPDATE':
            logger.warning(f"Ignoring non-UPDATE action: {user_data['action_type']}")
            channel.basic_ack(method.delivery_tag)
            return

        if user_data['uuid'].endswith('Z'):
            user_data['uuid'] = user_data['uuid'][:-1]
        if 'T' in user_data['uuid']:
            user_data['uuid'] = user_data['uuid'].replace('T', ' ')

        update_user(user_data)
        channel.basic_ack(method.delivery_tag)

    except Exception as e:
        logger.error(f"Message processing failed: {e}")
        channel.basic_nack(method.delivery_tag, requeue=False)

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
        queue = 'facturatie_user_update'
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_consume(
            queue=queue,
            on_message_callback=on_message,
            auto_ack=False
        )

        logger.info("Waiting for user update messages...")
        channel.start_consuming()

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
        channel.stop_consuming()
        connection.close()
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        raise

if __name__ == "__main__":
    start_consumer()
