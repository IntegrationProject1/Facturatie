import pika
import os
import logging
import xml.etree.ElementTree as ET
import mysql.connector

# Logging instellen
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Check of gebruiker al bestaat via UUID (= timestamp)
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

# XML parser
def parse_user_xml(xml_data):
    try:
        root = ET.fromstring(xml_data)

        business = root.find('Business')

        return {
            'action_type': root.find('ActionType').text,
            'uuid': root.find('UUID').text,
            'timestamp': root.find('TimeOfAction').text,
            'password': root.find('EncryptedPassword').text,
            'first_name': root.findtext('FirstName', default=''),
            'last_name': root.findtext('LastName', default=''),
            'phone': root.findtext('PhoneNumber', default=''),
            'email': root.findtext('EmailAddress', default=''),
            'company': business.findtext('BusinessName', default='') if business is not None else '',
            'company_email': business.findtext('BusinessEmail', default='') if business is not None else '',
            'address': business.findtext('RealAddress', default='') if business is not None else '',
            'vat': business.findtext('BTWNumber', default='') if business is not None else '',
            'invoice_address': business.findtext('FacturationAddress', default='') if business is not None else ''
        }
    except Exception as e:
        logger.error(f"XML parsing failed: {e}")
        raise

# Gebruiker toevoegen aan DB
def create_user(data):
    if user_exists(data['uuid']):
        logger.warning(f"User met timestamp {data['uuid']} bestaat al.")
        return False

    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        cursor = conn.cursor()
        sql = """
            INSERT INTO client (
                role, email, pass, status, first_name, last_name,
                phone, company, address_1, company_vat,
                timestamp, created_at
            ) VALUES (%s, %s, %s, 'active', %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        values = (
            'client',
            data['email'],
            data['password'],
            data['first_name'],
            data['last_name'],
            data['phone'],
            data['company'],
            data['address'],
            data['vat'],
            data['uuid']
        )
        cursor.execute(sql, values)
        conn.commit()
        logger.info(f"Gebruiker aangemaakt: {data['email']} ({data['uuid']})")
        return True
    except Exception as e:
        logger.error(f"Gebruiker aanmaken mislukt: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# Callback functie
def on_message(channel, method, properties, body):
    try:
        logger.info(f"Bericht ontvangen via {method.routing_key}")
        user_data = parse_user_xml(body.decode())

        if user_data['action_type'].upper() != 'CREATE':
            logger.warning(f"Ignoreren: niet-‘CREATE’ actie: {user_data['action_type']}")
            channel.basic_ack(method.delivery_tag)
            return

        # UUID format fix
        if user_data['uuid'].endswith('Z'):
            user_data['uuid'] = user_data['uuid'][:-1]
        if 'T' in user_data['uuid']:
            user_data['uuid'] = user_data['uuid'].replace('T', ' ')

        create_user(user_data)
        channel.basic_ack(method.delivery_tag)

    except Exception as e:
        logger.error(f"Fout tijdens verwerking: {e}")
        channel.basic_nack(method.delivery_tag, requeue=False)

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
        queues = ['facturatie_user_create']
        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)
            channel.basic_consume(
                queue=queue,
                on_message_callback=on_message,
                auto_ack=False
            )

        logger.info("Wachten op gebruikerscreatieberichten...")
        channel.start_consuming()

    except KeyboardInterrupt:
        logger.info("Consumer stoppen...")
        channel.stop_consuming()
        connection.close()
    except Exception as e:
        logger.error(f"Consumer mislukt: {e}")
        raise

if __name__ == "__main__":
    start_consumer()
