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

# Database connection
def get_db_connection():
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"]
    )

# Create XML message for RabbitMQ
def create_complete_xml(user):

    # Create XML structure with root element UserMessage
    xml = ET.Element("UserMessage")
    
    # Action info
    ET.SubElement(xml, "ActionType").text = "UPDATE"
    ET.SubElement(xml, "UserID").text = str(user['id'])
    ET.SubElement(xml, "TimeOfAction").text = datetime.utcnow().isoformat() + "Z"
    
    # Personal info
    if user.get('first_name'):
        ET.SubElement(xml, "FirstName").text = user['first_name']
    if user.get('last_name'):
        ET.SubElement(xml, "LastName").text = user['last_name']
    if user.get('email'):
        ET.SubElement(xml, "EmailAddress").text = user['email']
    if user.get('phone'):
        ET.SubElement(xml, "PhoneNumber").text = user['phone']
    
    # Business info (if any exists)
    if any(user.get(field) for field in ['company', 'company_vat', 'address1']):
        business = ET.SubElement(xml, "Business")
        
        if user.get('company'):
            ET.SubElement(business, "BusinessName").text = user['company']
        
        if user.get('email'):  # Fallback to user email
            ET.SubElement(business, "BusinessEmail").text = user['email']
        
        if user.get('address1'):
            address = ", ".join(filter(None, [
                user.get('address1'),
                user.get('address2'),
                user.get('postcode'),
                user.get('city'),
                user.get('country')
            ]))
            ET.SubElement(business, "RealAddress").text = address
            ET.SubElement(business, "FacturationAddress").text = address  # Same as real address unless specified
        
        if user.get('company_vat'):
            ET.SubElement(business, "BTWNumber").text = user['company_vat']
    
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(xml, encoding='unicode')

# Process updates from the queue and send to RabbitMQ
def process_updates():

    while True:
        try:
            with get_db_connection() as conn:
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute("""
                        SELECT 
                            c.id, c.first_name, c.last_name, c.email, c.phone,
                            c.company, c.company_vat, 
                            c.address1, c.address2, c.city, c.country, c.postcode
                        FROM client c
                        JOIN user_updates_queue q ON c.id = q.client_id
                        WHERE q.processed = FALSE
                        ORDER BY q.updated_at
                        LIMIT 50
                    """)
                    updates = cursor.fetchall()

            if not updates:
                time.sleep(10)
                continue

            for user in updates:
                try:
                    xml = create_complete_xml(user)
                    
                    # RabbitMQ connection
                    params = pika.ConnectionParameters(
                        host=os.environ["RABBITMQ_HOST"],
                        port=int(os.environ["RABBITMQ_PORT"]),
                        virtual_host="/",
                        credentials=pika.PlainCredentials(
                            os.environ["RABBITMQ_USER"],
                            os.environ["RABBITMQ_PASSWORD"]
                        ),
                        heartbeat=600
                    )
                    
                    # Send message to RabbitMQ
                    with pika.BlockingConnection(params) as connection:
                        channel = connection.channel()
                        channel.basic_publish(
                            exchange="user",
                            routing_key="facturatie.user.update",
                            body=xml,
                            properties=pika.BasicProperties(
                                delivery_mode=2  # Persistent message
                            )
                        )
                    
                    # Mark as processed
                    with get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                UPDATE user_updates_queue
                                SET processed = TRUE
                                WHERE client_id = %s
                            """, (user['id'],))
                            conn.commit()
                    
                    logger.info(f"Processed update for user {user['id']}")
                
                except pika.exceptions.AMQPError as e:  #error handling + logging
                    logger.error(f"RabbitMQ error for user {user['id']}: {e}")
                    time.sleep(5)  #wait 5 seconds before trying again to not overload RabbitMQ
                except Exception as e:  #error handling + logging
                    logger.error(f"Processing error for user {user['id']}: {e}")

            time.sleep(5)
        
        except mysql.connector.Error as e:  #error handling + logging
            logger.error(f"Database error: {e}")
            time.sleep(30)
        except Exception as e:  #error handling + logging
            logger.error(f"System error: {e}")
            time.sleep(60)

# Start the listener
if __name__ == "__main__":
    logger.info("Starting update listener with complete business fields")
    process_updates()