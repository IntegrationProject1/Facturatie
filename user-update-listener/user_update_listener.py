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
    """Establish MySQL connection"""
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"]
    )

# Process user updates from the queue and send them to RabbitMQ
def process_updates():

    while True:
        try:
            # Get pending updates
            conn = get_db_connection()
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT c.* FROM client c
                    JOIN user_updates_queue q ON c.id = q.client_id
                    WHERE q.processed = FALSE
                    ORDER BY q.updated_at
                    LIMIT 50
                """)
                updates = cursor.fetchall()

            if not updates:
                time.sleep(10)
                continue

            # Process each update
            for user in updates:
                # Generate XML
                xml = ET.Element("UserMessage")
                ET.SubElement(xml, "ActionType").text = "UPDATE"
                ET.SubElement(xml, "UserID").text = str(user['id'])
                ET.SubElement(xml, "TimeOfAction").text = datetime.utcnow().isoformat() + "Z"
                
                if user.get('first_name'):
                    ET.SubElement(xml, "FirstName").text = user['first_name']
                if user.get('last_name'):
                    ET.SubElement(xml, "LastName").text = user['last_name']
                if user.get('email'):
                    ET.SubElement(xml, "EmailAddress").text = user['email']
                
                xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(xml, encoding='unicode')

                # Send to RabbitMQ
                try:
                    params = pika.ConnectionParameters(
                        host=os.environ["RABBITMQ_HOST"],
                        port=int(os.environ["RABBITMQ_PORT"]),
                        virtual_host="/",
                        credentials=pika.PlainCredentials(
                            os.environ["RABBITMQ_USER"],
                            os.environ["RABBITMQ_PASSWORD"]
                        )
                    )
                    with pika.BlockingConnection(params) as connection:
                        channel = connection.channel()
                        channel.basic_publish(
                            exchange="user",
                            routing_key="facturatie.user.update",
                            body=xml_str
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
                
                except Exception as e:   #error handling + logging
                    logger.error(f"Failed to process user {user['id']}: {e}")

            time.sleep(5)   #every 5 seconds check for new updates to not overload the system

        except Exception as e:  #error handling + logging
            logger.error(f"System error: {e}")
            time.sleep(60)

if __name__ == "__main__":  #main function
    logger.info("Starting user update listener")
    process_updates()