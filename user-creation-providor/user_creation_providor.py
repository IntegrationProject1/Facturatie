import pika
import os
import xml.etree.ElementTree as ET
from datetime import datetime
import mysql.connector
import time
import logging

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

# Database connection
def get_db_connection():
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"]
    )

# Get new users from database that have not been processed yet
def get_new_users():

    # Establish connection
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # Get users that have not been processed yet and order by creation date (oldest first)
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
        return cursor.fetchall()
    except mysql.connector.Error as err:        #error handling + logging
        logger.error(f"Database error: {err}")
        return []
    finally:               #closing cursor and connection
        cursor.close()
        conn.close()

# Mark user as processed so it won't be processed again
def mark_as_processed(client_id):

    # Establish connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Insert user into processed_users table
    try:
        cursor.execute("""
            INSERT INTO processed_users (client_id, processed_at)
            VALUES (%s, NOW())
        """, (client_id,))
        conn.commit()
    except mysql.connector.Error as err:    #error handling + logging
        logger.error(f"Failed to mark user {client_id} as processed: {err}")
    finally:           #closing cursor and connection
        cursor.close()
        conn.close()

# Create XML message for RabbitMQ
def create_xml_message(user):

    # Create XML structure with root element UserMessage
    xml = ET.Element("UserMessage")
    
    # Action info
    ET.SubElement(xml, "ActionType").text = "CREATE"
    ET.SubElement(xml, "UserID").text = str(user['id'])
    ET.SubElement(xml, "TimeOfAction").text = datetime.utcnow().isoformat() + "Z"
    ET.SubElement(xml, "Password").text = user.get('pass', '')
    
    # Personal info
    if user.get('first_name'):
        ET.SubElement(xml, "FirstName").text = user['first_name']
    if user.get('last_name'):
        ET.SubElement(xml, "LastName").text = user['last_name']
    if user.get('phone'):
        ET.SubElement(xml, "PhoneNumber").text = user['phone']
    if user.get('email'):
        ET.SubElement(xml, "EmailAddress").text = user['email']
    
    # Business info (only if VAT or business name exists)
    if any(user.get(field) for field in ['business_name', 'btw_number', 'real_address']):
        business = ET.SubElement(xml, "Business")
        
        if user.get('business_name'):
            ET.SubElement(business, "BusinessName").text = user['business_name']
        
        # Business email (fallback to personal email)
        business_email = user.get('email', '')  # Using personal email as fallback
        if business_email:
            ET.SubElement(business, "BusinessEmail").text = business_email
        
        # Only include fields that exist
        if user.get('real_address'):
            ET.SubElement(business, "RealAddress").text = user['real_address']
        if user.get('btw_number'):
            ET.SubElement(business, "BTWNumber").text = user['btw_number']
        
        # Facturation address (fallback to real address)
        facturation_address = user.get('real_address', '')
        if facturation_address:
            ET.SubElement(business, "FacturationAddress").text = facturation_address
    
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(xml, encoding='unicode')    #returning the xml message

# Send XML message to multiple RabbitMQ queues
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

        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)
            channel.basic_publish(
                exchange="user",
                routing_key=f"user.create.{queue}",
                body=xml
            )
            logger.info(f"Sent XML message to {queue}")

        connection.close()
        return True
    except Exception as e:
        logger.error(f"RabbitMQ Error: {e}")
        return False

# Initialize database for safety and to avoid errors
# Create table processed_users if it does not exist
def initialize_database():

    # Establish connection
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
    except Exception as e:  #error handling + logging
        logger.error(f"Database initialization failed: {e}")
    finally:       #closing cursor and connection
        cursor.close()
        conn.close()

# Main loop
# Get new users every 5 seconds, create XML message and send to RabbitMQ
if __name__ == "__main__":
    initialize_database()
    logger.info("Starting user creation listener")  #logging
    
    while True:
        try:
            new_users = get_new_users()
            
            for user in new_users:
                xml = create_xml_message(user)
                if send_to_rabbitmq(xml):
                    mark_as_processed(user['id'])
                    logger.info(f"Processed user {user['id']}")
                else:
                    logger.error(f"Failed to process user {user['id']}")
            
            time.sleep(5)   # every 5 seconds the loop will run again to check for new users and process them
        except Exception as e: #error handling + logging
            logger.error(f"Processing error: {e}")
            time.sleep(60)  # if there is an error, the loop will wait for 60 seconds before running again