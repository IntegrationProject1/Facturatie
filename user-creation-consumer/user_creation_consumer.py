import pika
import os
import logging
import xml.etree.ElementTree as ET
import mysql.connector
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
open('logfile.log', 'w').close()

# Check if user exists
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
    except Exception as e:
        logger.error(f"Error checking user existence: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# Create user in FossBilling DB
def create_user(user_data):
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor()

    try:
        uuid_timestamp = user_data['uuid']

        if user_exists(uuid_timestamp):
            logger.warning(f"Client with timestamp {uuid_timestamp} already exists - skipping creation")
            return False

        query = """
            INSERT INTO client (
                email, pass, status, first_name, last_name, phone_cc, phone,
                company, address_1, city, state, postcode, country,
                currency, timestamp, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        """
        values = (
            user_data['email'],
            user_data['password'],  # moet reeds gehashed zijn!
            'active',
            user_data['first_name'],
            user_data['last_name'],
            user_data['phone_cc'],
            user_data['phone'],
            user_data['company'],
            user_data['address_1'],
            user_data['city'],
            user_data['state'],
            user_data['postcode'],
            user_data['country'],
            'EUR',
            uuid_timestamp
        )
        cursor.execute(query, values)
        conn.commit()
        logger.info(f"Created client: {uuid_timestamp}")
        return True

    except Exception as e:
        logger.error(f"User creation failed: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# Parse XML
def parse_user_create_xml(xml_data):
    try:
        root = ET.fromstring(xml_data)

        return {
            'action_type': root.find('ActionType').text,
            'uuid': root.find('UUID').text,
            'email': root.find('Email').text,
            'password': root.find('Password').text,
            'first_name': root.find('FirstName').text,
            'last_name': root.find('LastName').text,
            'phone_cc': root.find('PhoneCC').text,
            'phone': root.find('Phone').text,
            'company': root.find('Company').text,
            'address_1': root.find('Address1').text,
            'city': root.find('City').text,
            'state': root.find('State').text,
            'postcode': root.find('Postcode').text,
            'country': root.find('Country').text
        }
    except Exception as e:
        logger.error(f"XML parsing failed: {e}")
        raise

# Callback
def on_message(channel, method, properties, body):
    try:
        logger.info(f"Received message from {method.routing_key}")
        user_data = parse_user_create_xml(body.decode())

        if user_data['action_type'].upper() != 'CREATE':
            logger.warning(f"Ignoring non-CREATE action: {user_data['action_type']}")
            channel.basic_ack(method.delivery_tag)
            return

        # Clean UUID timestamp
        if user_data['uuid'].endswith('Z'):
            user_data['uuid'] = user_data['uuid'][:-1]
        if 'T' in user_data['uuid']:
            user_data['uuid'] = user_data['uuid'].replace('T', ' ')

        create_user(user_data)
        channel.basic_ack(method.delivery_tag)

    except Exception as e:
        logger.error(f"Message processing failed: {e}")
        channel.basic_nack(method.delivery_tag, requeue=False)

# Start consumer
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
        queue_name = 'facturatie_user_create'
        channel.queue_declare(queue=queue_name, durable=True)
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=on_message,
            auto_ack=False
        )

        logger.info("Waiting for user creation messages...")
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

