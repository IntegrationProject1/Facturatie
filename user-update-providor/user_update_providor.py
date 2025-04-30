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

def get_updated_users():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute("""
            SELECT 
                c.id,
                c.first_name, 
                c.last_name, 
                c.email, 
                c.pass, 
                c.phone,
                c.company AS business_name,
                c.company_vat AS btw_number,
                CONCAT_WS(', ', c.address_1, c.city, c.country) AS real_address,
                q.updated_at,
                c.timestamp
            FROM client c
            JOIN user_updates_queue q ON c.id = q.client_id
            WHERE q.processed = FALSE
            ORDER BY q.updated_at ASC
        """)
        users = cursor.fetchall()
        
        for user in users:
            updated_at = user['updated_at']
            if isinstance(updated_at, str):
                updated_at = datetime.strptime(updated_at, '%Y-%m-%d %H:%M:%S')
            user['timestamp'] = updated_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        return users
    except mysql.connector.Error as err:
        logger.error(f"Database error: {err}")
        return []
    finally:
        cursor.close()
        conn.close()

def create_xml_message(user):
    xml = ET.Element("UserMessage")
    
    # Action info
    ET.SubElement(xml, "ActionType").text = "UPDATE"
    ET.SubElement(xml, "UUID").text = user['timestamp']
    ET.SubElement(xml, "TimeOfAction").text = datetime.utcnow().isoformat() + "Z"
    ET.SubElement(xml, "EncryptedPassword").text = user.get('pass', '')
    
    # Personal info
    if user.get('first_name'):
        ET.SubElement(xml, "FirstName").text = user['first_name']
    if user.get('last_name'):
        ET.SubElement(xml, "LastName").text = user['last_name']
    if user.get('phone'):
        ET.SubElement(xml, "PhoneNumber").text = user['phone']
    if user.get('email'):
        ET.SubElement(xml, "EmailAddress").text = user['email']
    
    # Business info
    if any(user.get(field) for field in ['business_name', 'btw_number', 'real_address']):
        business = ET.SubElement(xml, "Business")
        
        if user.get('business_name'):
            ET.SubElement(business, "BusinessName").text = user['business_name']
        
        business_email = user.get('email', '')
        if business_email:
            ET.SubElement(business, "BusinessEmail").text = business_email
        
        if user.get('real_address'):
            ET.SubElement(business, "RealAddress").text = user['real_address']
            ET.SubElement(business, "FacturationAddress").text = user['real_address']
        
        if user.get('btw_number'):
            ET.SubElement(business, "BTWNumber").text = user['btw_number']
    
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(xml, encoding='unicode')

def send_to_rabbitmq(xml):
    queues = ["crm_user_update", "kassa_user_update", "frontend_user_update"]
    
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

        channel.exchange_declare(
            exchange="user",
            exchange_type="topic",
            durable=True
        )

        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)
            channel.queue_bind(
                exchange="user",
                queue=queue,
                routing_key=f"user.update.{queue}"
            )
            channel.basic_publish(
                exchange="user",
                routing_key=f"user.update.{queue}",
                body=xml,
                properties=pika.BasicProperties(
                    delivery_mode=2  # Make messages persistent
                )
            )
            logger.info(f"Sent XML message to {queue}")

        connection.close()
        return True
    except Exception as e:
        logger.error(f"RabbitMQ Error: {e}")
        return False

def mark_as_processed(client_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            UPDATE user_updates_queue
            SET processed = TRUE
            WHERE client_id = %s
        """, (client_id,))
        conn.commit()
    except mysql.connector.Error as err:
        logger.error(f"Failed to mark update {client_id} as processed: {err}")
    finally:
        cursor.close()
        conn.close()

def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_updates_queue (
                id INT AUTO_INCREMENT PRIMARY KEY,
                client_id INT NOT NULL UNIQUE,
                updated_at DATETIME(6) NOT NULL,
                processed BOOLEAN DEFAULT FALSE,
                INDEX (client_id),
                INDEX (processed)
            )
        """)
        conn.commit()
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    initialize_database()
    logger.info("Starting user update provider")
    
    while True:
        try:
            updated_users = get_updated_users()
            
            for user in updated_users:
                xml = create_xml_message(user)
                if send_to_rabbitmq(xml):
                    mark_as_processed(user['id'])
                    logger.info(f"Processed user update {user['id']} with timestamp ID {user['timestamp']}")
                else:
                    logger.error(f"Failed to process user update {user['id']}")
            
            time.sleep(5)
        except Exception as e:
            logger.error(f"Processing error: {e}")
            time.sleep(60)