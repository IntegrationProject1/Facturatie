import pika
import os
import logging
import xml.etree.ElementTree as ET
import mysql.connector
from datetime import datetime

# Configure logging with debug level
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more detailed logging
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection helper function
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

# Check if user exists and return current data if they do
def get_current_user_data(uuid_timestamp):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        logger.debug(f"Checking for existing user with timestamp: {uuid_timestamp}")
        cursor.execute("""
            SELECT 
                id, email, pass, first_name, last_name, 
                phone, company, address_1 as address, company_vat as vat
            FROM client 
            WHERE timestamp = %s
        """, (uuid_timestamp,))
        user = cursor.fetchone()
        
        if user:
            logger.debug(f"Found existing user: {user}")
            return user
        logger.warning(f"No user found with timestamp: {uuid_timestamp}")
        return None
    finally:
        cursor.close()
        conn.close()

# XML parser (similar to creation but handles UPDATE action type)
def parse_user_xml(xml_data):
    try:
        logger.debug("Parsing XML data")
        root = ET.fromstring(xml_data)
        
        # Validate action type
        action_type = root.find('ActionType').text.upper()
        if action_type != 'UPDATE':
            raise ValueError(f"Invalid action type for update consumer: {action_type}")

        business = root.find('Business')
        
        # Parse all possible fields (most are optional)
        parsed_data = {
            'action_type': action_type,
            'uuid': root.find('UUID').text,
            'timestamp': root.find('TimeOfAction').text,
            'password': root.findtext('EncryptedPassword'),  # Optional for updates
            'first_name': root.findtext('FirstName'),
            'last_name': root.findtext('LastName'),
            'phone': root.findtext('PhoneNumber'),
            'email': root.findtext('EmailAddress'),
            'company': business.findtext('BusinessName') if business is not None else None,
            'company_email': business.findtext('BusinessEmail') if business is not None else None,
            'address': business.findtext('RealAddress') if business is not None else None,
            'vat': business.findtext('BTWNumber') if business is not None else None,
            'invoice_address': business.findtext('FacturationAddress') if business is not None else None
        }
        
        logger.debug(f"Parsed XML data: {parsed_data}")
        return parsed_data
    except Exception as e:
        logger.error(f"XML parsing failed: {e}")
        raise

# Update user in database
def update_user(data):
    # First get current user data
    current_data = get_current_user_data(data['uuid'])
    if not current_data:
        logger.error(f"Cannot update - user with timestamp {data['uuid']} does not exist")
        return False

    # Prepare update fields - only include fields that are provided in the XML
    update_fields = {}
    
    # Basic fields
    if data['password'] is not None:
        update_fields['pass'] = data['password']
    if data['first_name'] is not None:
        update_fields['first_name'] = data['first_name']
    if data['last_name'] is not None:
        update_fields['last_name'] = data['last_name']
    if data['phone'] is not None:
        update_fields['phone'] = data['phone']
    if data['email'] is not None:
        update_fields['email'] = data['email']
    
    # Business fields
    if data['company'] is not None:
        update_fields['company'] = data['company']
    if data['address'] is not None:
        update_fields['address_1'] = data['address']
    if data['vat'] is not None:
        update_fields['company_vat'] = data['vat']
    
    # If no fields to update, log and return
    if not update_fields:
        logger.info("No fields to update - all fields in XML were empty")
        return True
    
    # Build the dynamic SQL update query
    set_clause = ", ".join([f"{field} = %s" for field in update_fields.keys()])
    values = list(update_fields.values())
    values.append(data['uuid'])  # For WHERE clause
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        sql = f"""
            UPDATE client 
            SET {set_clause}, updated_at = NOW()
            WHERE timestamp = %s
        """
        
        logger.debug(f"Executing update query: {sql}")
        logger.debug(f"With values: {values}")
        
        cursor.execute(sql, values)
        conn.commit()
        
        logger.info(f"Successfully updated user with timestamp: {data['uuid']}")
        return True
    except Exception as e:
        logger.error(f"User update failed: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def on_message(channel, method, properties, body):
    try:
        logger.info(f"Received message via {method.routing_key}")
        logger.debug(f"Message body: {body.decode()}")
        
        user_data = parse_user_xml(body.decode())

        # Check action type
        if user_data['action_type'] != 'UPDATE':
            logger.warning(f"Ignoring non-UPDATE action: {user_data['action_type']}")
            channel.basic_ack(method.delivery_tag)
            return

        # Format UUID/timestamp (same as creation consumer)
        if user_data['uuid'].endswith('Z'):
            user_data['uuid'] = user_data['uuid'][:-1]
        if 'T' in user_data['uuid']:
            user_data['uuid'] = user_data['uuid'].replace('T', ' ')
        
        update_user(user_data)
        channel.basic_ack(method.delivery_tag)
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        channel.basic_nack(method.delivery_tag, requeue=False)

# Start the consumer
def start_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST"),
        port=int(os.getenv("RABBITMQ_PORT")),
        credentials=pika.PlainCredentials(
            os.getenv("RABBITMQ_USER"),
            os.getenv("RABBITMQ_PASSWORD")
        ),
        heartbeat=600,
        blocked_connection_timeout=300
    ))
    channel = connection.channel()

    try:
        # Declare the update queue
        queue_name = 'facturatie_user_update'
        channel.queue_declare(queue=queue_name, durable=True)
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
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        raise

if __name__ == "__main__":
    logger.info("Starting update consumer...")
    start_consumer()