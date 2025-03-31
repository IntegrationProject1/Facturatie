import pika
import os
import logging
import xml.etree.ElementTree as ET
import mysql.connector

# Configure logging for the application
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

# Update user details in the database
def update_user(user_data):
    conn = None
    cursor = None
    try:
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
        
        # Execute the update query with user data
        cursor.execute(update_query, (
            user_data['first_name'],
            user_data['last_name'],
            user_data['phone'],
            user_data['business_name'],
            user_data['vat_number'],
            user_data['address'],
            extract_city(user_data['address']),
            extract_postcode(user_data['address']),
            user_data['email']
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

# Extract city from the address string
def extract_city(address):
    parts = address.split(',')
    if len(parts) >= 3:
        return parts[-2].strip()
    return ''

# Extract postcode from the address string
def extract_postcode(address):
    parts = address.split(',')
    if len(parts) >= 3:
        return parts[-1].strip().split(' ')[0]
    return ''

# Parse XML data to extract user information
def parse_user_xml(xml_data):
    try:
        root = ET.fromstring(xml_data)
        business = root.find('Business')
        
        return {
            'action_type': root.find('ActionType').text,
            'email': root.find('EmailAddress').text,
            'first_name': root.find('FirstName').text,
            'last_name': root.find('LastName').text,
            'phone': root.find('PhoneNumber').text,
            'business_name': business.find('BusinessName').text if business is not None else '',
            'vat_number': business.find('BTWNumber').text if business is not None else '',
            'address': business.find('RealAddress').text if business is not None else ''
        }
    except Exception as e:
        logger.error(f"XML parsing failed: {e}")
        raise

# Handle incoming RabbitMQ messages
def on_message(channel, method, properties, body):
    try:
        logger.info(f"Received update message from {method.routing_key}")
        
        # Parse the XML message
        user_data = parse_user_xml(body.decode())
        
        # Ignore non-UPDATE actions
        if user_data['action_type'].upper() != 'UPDATE':
            logger.warning(f"Ignoring non-UPDATE action: {user_data['action_type']}")
            channel.basic_ack(method.delivery_tag)
            return
            
        # Update the user in the database
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
        # Declare queues to listen for user updates
        queues = ['crm_user_update', 'frontend_user_update', 'kassa_user_update']
        for queue in queues:
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
        logger.info("Consumer stopped.")
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        raise

# Main entry point to start the consumer
if __name__ == "__main__":
    start_consumer()