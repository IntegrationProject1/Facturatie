import pika
import os
import xml.etree.ElementTree as ET
from datetime import datetime
import mysql.connector
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Connect to MySQL using compose environment variables"""
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"]
    )

def get_new_users():
    """Fetch unprocessed users from client table"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute("""
            SELECT c.* FROM client c
            LEFT JOIN processed_users p ON c.id = p.client_id
            WHERE p.client_id IS NULL
            ORDER BY c.created_at ASC
        """)
        return cursor.fetchall()
    except mysql.connector.Error as err:
        logger.error(f"Database error: {err}")
        return []
    finally:
        cursor.close()
        conn.close()

def mark_as_processed(client_id):
    """Mark user as processed"""
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

def create_xml_message(user):
    """Generate XML from client data"""
    xml = ET.Element("UserMessage")
    
    # Required fields
    ET.SubElement(xml, "ActionType").text = "CREATE"
    ET.SubElement(xml, "UserID").text = str(user['id'])
    ET.SubElement(xml, "TimeOfAction").text = datetime.utcnow().isoformat() + "Z"
    ET.SubElement(xml, "Password").text = user.get('pass', '')
    
    # Optional fields
    if user.get('first_name'):
        ET.SubElement(xml, "FirstName").text = user['first_name']
    if user.get('last_name'):
        ET.SubElement(xml, "LastName").text = user['last_name']
    if user.get('phone'):
        ET.SubElement(xml, "PhoneNumber").text = user['phone']
    if user.get('email'):
        ET.SubElement(xml, "EmailAddress").text = user['email']
    
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(xml, encoding='unicode')

def send_to_rabbitmq(xml):
    """Send XML to RabbitMQ"""
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
        
        channel.queue_declare(queue="facturatie_user_create", durable=True)
        channel.basic_publish(
            exchange="user",
            routing_key="facturatie.user.create",
            body=xml
        )
        
        connection.close()
        return True
    except Exception as e:
        logger.error(f"RabbitMQ Error: {e}")
        return False

def initialize_database():
    """Create processing table if missing"""
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

if __name__ == "__main__":
    initialize_database()
    logger.info("Starting user creation listener")
    
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
            
            time.sleep(5)
        except Exception as e:
            logger.error(f"Processing error: {e}")
            time.sleep(60)