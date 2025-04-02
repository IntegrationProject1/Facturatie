import pika
import os
import logging
import xml.etree.ElementTree as ET
import mysql.connector
import time

# For logging and debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Disable pika logging
pika_logger = logging.getLogger("pika")
pika_logger.handlers.clear()  # Removes any existing handlers
pika_logger.propagate = False
pika_logger.setLevel(logging.WARNING)  # Only show warnings and errors

open('logfile.log', 'w').close()  # Clear previous log file

# Load environment variables and check if user already exists
def user_exists(email):
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT id FROM client WHERE email = %s", (email,))
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
        if user_exists(user_data['email']):
            logger.warning(f"User with email {user_data['email']} already exists")
            return False
        
        # Prepare data for FossBilling schema
        from datetime import datetime
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        insert_query = """
        INSERT INTO client (
            role, email, status, first_name, last_name, 
            phone, company, company_vat, address_1, 
            city, state, postcode, country, currency, 
            created_at, updated_at, is_processed
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Use None for optional fields if they are not provided
        cursor.execute(insert_query, (
            'client',                              # role
            user_data['email'],                    # email (required)
            'active',                              # status
            user_data['first_name'],               # first_name (required)
            user_data['last_name'],                # last_name (required)
            user_data.get('phone'),                # phone (optional)
            user_data.get('business_name'),        # company (optional)
            user_data.get('vat_number'),           # company_vat (optional)
            user_data.get('address'),              # address_1 (optional)
            extract_city(user_data.get('address')) if user_data.get('address') else None,  # city (optional)
            '',                                    # state (optional, empty string)
            extract_postcode(user_data.get('address')) if user_data.get('address') else None,  # postcode (optional)
            'BE',                                  # country (default to Belgium)
            'EUR',                                 # currency (default to EUR)
            now,                                   # created_at
            now,                                   # updated_at
            0                                      # is_processed
        ))
        
        conn.commit()
        logger.info(f"Created new client: {user_data['email']}")
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
        logger.info(f"Parsing XML: {xml_data}")  # Log the XML before parsing
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
            'user_id': safe_find(root, 'UserID'),
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
        logger.info("Message failed and will not be requeued.")

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
    channel.basic_qos(prefetch_count=1)  # Prevents RabbitMQ from sending multiple messages before acknowledging
    
    try:
        # Explicitly declare queue before consuming
        queue_name = 'facturatie_user_create'
        channel.queue_declare(queue=queue_name, durable=True)

        # Add a short delay before consuming
        logger.info("Waiting 2 seconds before consuming to ensure queue is ready...")
        time.sleep(2)

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