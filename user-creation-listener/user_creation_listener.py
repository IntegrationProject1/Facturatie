import pika
import os
import xml.etree.ElementTree as ET
from datetime import datetime
import mysql.connector
import time
import logging

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

def get_user_data(client_id):
    """Fetch user data from FOSSBilling database"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute("""
            SELECT c.*, 
                   a.company as business_name, 
                   a.company_vat as btw_number,
                   CONCAT_WS(', ', a.address1, a.city, a.country) as real_address
            FROM client c
            LEFT JOIN client_order o ON c.id = o.client_id
            LEFT JOIN invoice_order io ON o.id = io.order_id
            LEFT JOIN invoice i ON io.invoice_id = i.id
            LEFT JOIN invoice_address a ON i.id = a.invoice_id
            WHERE c.id = %s
            LIMIT 1
        """, (client_id,))
        return cursor.fetchone()
    finally:
        cursor.close()
        conn.close()

def create_xml_message(user_data):
    """Generate XML in the required format"""
    user_message = ET.Element("UserMessage")
    
    ET.SubElement(user_message, "ActionType").text = "CREATE"
    ET.SubElement(user_message, "UserID").text = str(user_data['id'])
    ET.SubElement(user_message, "TimeOfAction").text = datetime.utcnow().isoformat() + "Z"
    ET.SubElement(user_message, "Password").text = user_data.get('pass', '')
    
    if user_data.get('first_name'):
        ET.SubElement(user_message, "FirstName").text = user_data['first_name']
    if user_data.get('last_name'):
        ET.SubElement(user_message, "LastName").text = user_data['last_name']
    if user_data.get('phone'):
        ET.SubElement(user_message, "PhoneNumber").text = user_data['phone']
    if user_data.get('email'):
        ET.SubElement(user_message, "EmailAddress").text = user_data['email']
    
    if any(k in user_data for k in ['business_name', 'btw_number']):
        business = ET.SubElement(user_message, "Business")
        if user_data.get('business_name'):
            ET.SubElement(business, "BusinessName").text = user_data['business_name']
        if user_data.get('email'):
            ET.SubElement(business, "BusinessEmail").text = user_data['email']
        if user_data.get('real_address'):
            ET.SubElement(business, "RealAddress").text = user_data['real_address']
        if user_data.get('btw_number'):
            ET.SubElement(business, "BTWNumber").text = user_data['btw_number']
        if user_data.get('real_address'):
            ET.SubElement(business, "FacturationAddress").text = user_data['real_address']
    
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(user_message, encoding='unicode')

def send_to_rabbitmq(xml_message, user_data):
    """Send XML to RabbitMQ using compose environment variables"""
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
    
    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        
        channel.queue_declare(queue="facturatie_user_create", durable=True)
        channel.basic_publish(
            exchange="user",
            routing_key="facturatie.user.create",
            body=xml_message
        )
        
        logger.info(f"Sent user {user_data['id']} to RabbitMQ")
        connection.close()
        return True
    except Exception as e:
        logger.error(f"RabbitMQ Error: {e}")
        return False

def process_new_users():
    """Main processing loop"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute("""
            SELECT id FROM client 
            WHERE id NOT IN (
                SELECT client_id FROM processed_user_notifications
            )
            ORDER BY created_at DESC
        """)
        
        for row in cursor.fetchall():
            user_data = get_user_data(row['id'])
            if user_data:
                xml = create_xml_message(user_data)
                if send_to_rabbitmq(xml, user_data):

                    cursor.execute("""
                        INSERT INTO processed_user_notifications 
                        (client_id, processed_at) 
                        VALUES (%s, NOW())
                    """, (row['id'],))
                    conn.commit()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_user_notifications (
                id INT AUTO_INCREMENT PRIMARY KEY,
                client_id INT NOT NULL UNIQUE,
                processed_at DATETIME NOT NULL
            )
        """)
        conn.commit()
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

    logger.info("Starting user creation listener")
    while True:
        try:
            process_new_users()
            time.sleep(5)  
        except Exception as e:
            logger.error(f"Processing error: {e}")
            time.sleep(60)  