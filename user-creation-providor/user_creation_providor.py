import pika
import os
import xml.etree.ElementTree as ET
from datetime import datetime
import mysql.connector
import time
import logging

# Logging configuratie
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connectie
def get_db_connection():
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"]
    )

# Nieuwe users ophalen (oude functionaliteit behouden)
def get_new_users():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT 
                c.id, c.first_name, c.last_name, c.email, c.pass, c.phone, c.created_at,
                c.company AS business_name,
                c.company_vat AS btw_number,
                CONCAT_WS(', ', c.address_1, c.city, c.country) AS real_address
            FROM client c
            LEFT JOIN processed_users p ON c.id = p.client_id
            WHERE p.client_id IS NULL
            ORDER BY c.created_at ASC
        """)
        users = cursor.fetchall()

        for user in users:
            created_at = user['created_at']
            if isinstance(created_at, str):
                created_at = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            user['timestamp'] = created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        return users
    except mysql.connector.Error as err:
        logger.error(f"Database error: {err}")
        return []
    finally:
        cursor.close()
        conn.close()

# Verwijderde gebruikers ophalen
def get_deleted_users():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT id, client_id, timestamp 
            FROM user_deletion_notifications 
            WHERE processed = 0 
            ORDER BY timestamp ASC
        """)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

# Markeer user creation als verwerkt
def mark_as_processed(client_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO processed_users (client_id, processed_at)
            VALUES (%s, NOW())
        """, (client_id,))
        conn.commit()
    except mysql.connector.Error as err:
        logger.error(f"Failed to mark user {client_id} as processed: {err}")
    finally:
        cursor.close()
        conn.close()

# Markeer deletion als verwerkt
def mark_user_deletion_as_processed(record_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE user_deletion_notifications SET processed = 1 WHERE id = %s", (record_id,))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# XML aanmaken voor nieuwe user creation
def create_xml_message(user):
    xml = ET.Element("UserMessage")
    ET.SubElement(xml, "ActionType").text = "CREATE"
    ET.SubElement(xml, "UUID").text = user['timestamp']
    ET.SubElement(xml, "TimeOfAction").text = datetime.utcnow().isoformat() + "Z"
    ET.SubElement(xml, "EncryptedPassword").text = user.get('pass', '')

    if user.get('first_name'):
        ET.SubElement(xml, "FirstName").text = user['first_name']
    if user.get('last_name'):
        ET.SubElement(xml, "LastName").text = user['last_name']
    if user.get('phone'):
        ET.SubElement(xml, "PhoneNumber").text = user['phone']
    if user.get('email'):
        ET.SubElement(xml, "EmailAddress").text = user['email']

    if any(user.get(field) for field in ['business_name', 'btw_number', 'real_address']):
        business = ET.SubElement(xml, "Business")
        if user.get('business_name'):
            ET.SubElement(business, "BusinessName").text = user['business_name']
        business_email = user.get('email', '')
        if business_email:
            ET.SubElement(business, "BusinessEmail").text = business_email
        if user.get('real_address'):
            ET.SubElement(business, "RealAddress").text = user['real_address']
        if user.get('btw_number'):
            ET.SubElement(business, "BTWNumber").text = user['btw_number']
        facturation_address = user.get('real_address', '')
        if facturation_address:
            ET.SubElement(business, "FacturationAddress").text = facturation_address

    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(xml, encoding='unicode')

# XML logbericht voor controlroom
def send_log_to_controlroom(client_id, timestamp):
    log = ET.Element("Log")
    ET.SubElement(log, "ServiceName").text = "Facturatie"
    ET.SubElement(log, "Status").text = "INFO"
    ET.SubElement(log, "Message").text = f"User deleted with ID {client_id} at {timestamp}"

    xml_string = '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(log, encoding='unicode')

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

        channel.exchange_declare(exchange="log_monitoring", exchange_type="topic", durable=True)

        channel.basic_publish(
            exchange="log_monitoring",
            routing_key="controlroom.log.event",
            body=xml_string,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        logger.info(f"Log sent to controlroom for client {client_id}")
        connection.close()
    except Exception as e:
        logger.error(f"Failed to send log for client {client_id}: {e}")

# Berichten verzenden naar RabbitMQ voor creation
def send_to_rabbitmq(xml):
    queues = ["crm_user_create", "kassa_user_create", "frontend_user_create"]

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

        channel.exchange_declare(exchange="user", exchange_type="topic", durable=True)

        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)
            channel.queue_bind(exchange="user", queue=queue, routing_key=f"user.create.{queue}")
            channel.basic_publish(
                exchange="user",
                routing_key=f"user.create.{queue}",
                body=xml,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            logger.info(f"Sent XML message to {queue}")

        connection.close()
        return True
    except Exception as e:
        logger.error(f"RabbitMQ Error: {e}")
        return False

# Init database
def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                client_id INT NOT NULL UNIQUE,
                processed_at DATETIME NOT NULL
            )
        """)
        conn.commit()
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
    finally:
        cursor.close()
        conn.close()

# MAIN LOOP
if __name__ == "__main__":
    initialize_database()
    logger.info("Starting user deletion provider")

    while True:
        try:
            new_users = get_new_users()

            for user in new_users:
                xml = create_xml_message(user)
                if send_to_rabbitmq(xml):
                    mark_as_processed(user['id'])
                    logger.info(f"Processed user {user['id']} with timestamp ID {user['timestamp']}")
                else:
                    logger.error(f"Failed to process user {user['id']}")

            deleted_users = get_deleted_users()

            for deleted in deleted_users:
                send_log_to_controlroom(deleted["client_id"], deleted["timestamp"])
                mark_user_deletion_as_processed(deleted["id"])
                logger.info(f"Processed deletion log for client {deleted['client_id']}")

            time.sleep(5)
        except Exception as e:
            logger.error(f"Processing error: {e}")
            time.sleep(60)
