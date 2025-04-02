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

# Establish a connection to the MySQL database
def get_db_connection():

    try:
        return mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

# Fetch current user data from the database
def get_current_user_data(email):

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT first_name, last_name, phone, company, company_vat, 
                   address_1, city, postcode 
            FROM client 
            WHERE email = %s
        """, (email,))
        
        result = cursor.fetchone()
        if not result:
            raise ValueError(f"No user found with email: {email}")
            
        return result
        
    except Exception as e:
        logger.error(f"Failed to fetch current user data: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

# Update user details in the database
def update_user(user_data):

    conn = None
    cursor = None
    try:
        # Get current data for fields not being updated
        current_data = get_current_user_data(user_data['email'])
        
        # Prepare update fields - only update what's provided in the XML
        update_fields = {
            'first_name': user_data.get('first_name') or current_data['first_name'],
            'last_name': user_data.get('last_name') or current_data['last_name'],
            'phone': user_data.get('phone') or current_data['phone'],
            'company': user_data.get('business_name') or current_data['company'],
            'company_vat': user_data.get('vat_number') or current_data['company_vat'],
            'address_1': user_data.get('address') or current_data['address_1'],
            'city': extract_city(user_data.get('address')) if user_data.get('address') else current_data['city'],
            'postcode': extract_postcode(user_data.get('address')) if user_data.get('address') else current_data['postcode'],
            'email': user_data['email']
        }
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        update_query = """
        UPDATE client SET
            first_name = %s,
            last_name = %s,
            phone = %s,
            company = %s,
            company_vat = %s,
            address_1 = %s,
            city = %s,
            postcode = %s,
            updated_at = NOW()
        WHERE email = %s
        """
        
        cursor.execute(update_query, (
            update_fields['first_name'],
            update_fields['last_name'],
            update_fields['phone'],
            update_fields['company'],
            update_fields['company_vat'],
            update_fields['address_1'],
            update_fields['city'],
            update_fields['postcode'],
            update_fields['email']
        ))
        
        conn.commit()
        logger.info(f"Updated client: {user_data['email']}")
        return True
        
    except Exception as e:
        logger.error(f"Client update failed: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

# Extract the city from the address string
def extract_city(address):

    if not address:
        return ''
    parts = address.split(',')
    if len(parts) >= 3:
        return parts[-2].strip()
    return ''

# Extract the postcode from the address string
def extract_postcode(address):

    if not address:
        return ''
    parts = address.split(',')
    if len(parts) >= 3:
        return parts[-1].strip().split(' ')[0]
    return ''

# Parse XML data to extract user information
def parse_user_xml(xml_data):

    try:
        root = ET.fromstring(xml_data)
        business = root.find('Business')
        
        # Only include fields that are present in the XML
        user_data = {
            'action_type': root.find('ActionType').text,
            'email': root.find('EmailAddress').text
        }
        
        # Optional fields
        if root.find('FirstName') is not None:
            user_data['first_name'] = root.find('FirstName').text
        if root.find('LastName') is not None:
            user_data['last_name'] = root.find('LastName').text
        if root.find('PhoneNumber') is not None:
            user_data['phone'] = root.find('PhoneNumber').text
            
        if business is not None:
            if business.find('BusinessName') is not None:
                user_data['business_name'] = business.find('BusinessName').text
            if business.find('BTWNumber') is not None:
                user_data['vat_number'] = business.find('BTWNumber').text
            if business.find('RealAddress') is not None:
                user_data['address'] = business.find('RealAddress').text
        
        return user_data
    except Exception as e:
        logger.error(f"XML parsing failed: {e}")
        raise

# Handle incoming RabbitMQ messages
def on_message(channel, method, properties, body):

    try:
        logger.info(f"Received update message from {method.routing_key}")
        
        user_data = parse_user_xml(body.decode())
        
        if user_data['action_type'].upper() != 'UPDATE':
            logger.warning(f"Ignoring non-UPDATE action: {user_data['action_type']}")
            channel.basic_ack(method.delivery_tag)
            return
            
        update_user(user_data)
        channel.basic_ack(method.delivery_tag)
        
    except Exception as e:
        logger.error(f"Message processing failed: {e}")
        channel.basic_nack(method.delivery_tag, requeue=False)

# Start the RabbitMQ consumer to listen for update messages
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
        # Explicitly declare queue before consuming
        queue_name = 'facturatie_user_update'
        channel.queue_declare(queue=queue_name, durable=True)

        # Add a short delay before consuming
        logger.info("Waiting 2 seconds before consuming to ensure queue is ready...")
        time.sleep(2)

        channel.basic_consume(
            queue=queue_name,
            on_message_callback=on_message,
            auto_ack=False
        )

        logger.info("Waiting for user update messages...")
        channel.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
        channel.stop_consuming()
        connection.close()
        logger.info("Consumer stopped.")
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        raise

# Main entry point to start the consumer
if __name__ == "__main__":
    start_consumer()