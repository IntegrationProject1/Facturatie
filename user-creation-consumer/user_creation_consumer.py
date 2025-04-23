import pika
import os
import logging
import xml.etree.ElementTree as ET
import mysql.connector 
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
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
        parsed_timestamp = datetime.strptime(now, "%H%M%S%f")
        user_data['timestamp'] = parsed_timestamp.strftime("%H%M%S%f")
        
        insert_query = """
        INSERT INTO client (
            role, email, status, first_name, last_name, 
            phone, company, company_vat, address_1, 
            city, state, postcode, country, currency, 
            created_at, updated_at, is_processed
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            0                                     # is_processed
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
        
        business = root.find('Business')
        
        return {
            'action_type': root.find('ActionType').text,
            'user_id': root.find('UserID').text,
            'action_time': root.find('TimeOfAction').text,
            'first_name': root.find('FirstName').text,
            'last_name': root.find('LastName').text,
            'phone': root.find('PhoneNumber').text,
            'email': root.find('EmailAddress').text,
            'business_name': business.find('BusinessName').text,
            'business_email': business.find('BusinessEmail').text,
            'address': business.find('RealAddress').text,
            'vat_number': business.find('BTWNumber').text,
            'billing_address': business.find('FacturationAddress').text
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