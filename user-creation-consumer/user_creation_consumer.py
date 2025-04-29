import pika
import os
import logging
import xml.etree.ElementTree as ET
import mysql.connector
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
 
# Load environment variables and check if user already exists
def user_exists(timestamp):
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor()
   
    try:
        cursor.execute("SELECT id FROM client WHERE timestamp = %s", (timestamp,))
        return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"Error checking user existence: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
 
# Extract city and postcode from address
def extract_city(address):
 
    parts = address.split(',')
    if len(parts) >= 3:
        return parts[-2].strip()
    return ''
 
# Extract postcode from address string
def extract_postcode(address):
 
    parts = address.split(',')
    if len(parts) >= 3:
        return parts[-1].strip().split(' ')[0]
    return ''
 
# Create user in FossBilling database
def create_user(user_data):
 
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor()
   
    try:
        # Check if user already exists
        if user_exists(user_data['timestamp']):
            logger.warning(f"User with timestamp {user_data['timestamp']} already exists")
            return False
       
        # Prepare data for FossBilling schema
        from datetime import datetime
        now = user_data['timestamp']
        parsed_timestamp = datetime.strptime(now, "%Y-%m-%dT%H:%M:%S.%fZ")
        user_data['timestamp'] = parsed_timestamp.strftime("%H%M%S%f")
       
        insert_query = """
        INSERT INTO client (
            role, email, status, first_name, last_name,
            phone, company, company_vat, address_1,
            city, state, postcode, country, currency,
            created_at, updated_at, is_processed, timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
       
        cursor.execute(insert_query, (
            'client',                              # role
            user_data['email'],                    # email
            'active',                              # status
            user_data['first_name'],               # first_name
            user_data['last_name'],               # last_name
            user_data['phone'],                   # phone
            user_data['business_name'],           # company
            user_data['vat_number'],               # company_vat
            user_data['address'],                  # address_1
            extract_city(user_data['address']),    # city (extracted from address)
            '',                                    # state (empty as it's in address)
            extract_postcode(user_data['address']), # postcode
            'BE',                                 # country (default to Belgium)
            'EUR',                                # currency
            now,                                  # created_at
            now,                                  # updated_at
            0,                                     # is_processed
            user_data['uuid']                # timestamp
        ))
       
        conn.commit()
        logger.info(f"Created new client: {user_data['timestamp']}")
        return True
       
    except Exception as e:
        logger.error(f"Client creation failed: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
 
# Parse XML message
def parse_user_xml(xml_data):
    try:
        root = ET.fromstring(xml_data)

        # Utility function to safely get text from XML elements
        def safe_find(element, tag):
            found_element = element.find(tag)
            if found_element is not None:
                return found_element.text
            else:
                logger.warning(f"Missing expected XML element: {tag}")
                return None

        business = root.find('Business')

        return {
            'action_type': safe_find(root, 'ActionType'),
            'timestamp': safe_find(root, 'UUID'),
            'uuid': safe_find(root, 'UUID'),
            'action_time': safe_find(root, 'TimeOfAction'),
            'first_name': safe_find(root, 'FirstName'),
            'last_name': safe_find(root, 'LastName'),
            'phone': safe_find(root, 'PhoneNumber'),
            'email': safe_find(root, 'EmailAddress'),
            'business_name': safe_find(business, 'BusinessName') if business else None,
            'business_email': safe_find(business, 'BusinessEmail') if business else None,
            'address': safe_find(business, 'RealAddress') if business else None,
            'vat_number': safe_find(business, 'BTWNumber') if business else None,
            'billing_address': safe_find(business, 'FacturationAddress') if business else None
        }
    except Exception as e:
        logger.error(f"XML parsing failed: {e}")
        raise
 
# Callback for when a message is received from RabbitMQ
def on_message(channel, method, properties, body):
 
    try:
        logger.info(f"Received message from {method.routing_key}")
       
        # Parse XML
        user_data = parse_user_xml(body.decode())
       
        # Only process CREATE actions
        if user_data['action_type'].upper() != 'CREATE':
            logger.warning(f"Ignoring non-CREATE action: {user_data['action_type']}")
            channel.basic_ack(method.delivery_tag)
            return
           
        # Create user in database
        create_user(user_data)
       
        # Acknowledge message
        channel.basic_ack(method.delivery_tag)
       
    except Exception as e:
        logger.error(f"Message processing failed: {e}")
        channel.basic_nack(method.delivery_tag, requeue=False)
 
# Start the RabbitMQ consumer
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
        # Declare all queues we want to listen to
        queues = ['facturatie_user_create']
        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)
            channel.basic_consume(
                queue=queue,
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
