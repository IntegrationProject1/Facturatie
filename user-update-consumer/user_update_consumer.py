import pika
import xml.etree.ElementTree as ET
import logging
import mysql.connector.pooling
import os
import time
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
)
logger = logging.getLogger(__name__)

def update_client(email, xml_data):
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM client WHERE email = %s", (email,))
    user = cursor.fetchone()

    try:
        root = ET.fromstring(xml_data)
        user_id = root.find("UserID").text
        email = root.find("EmailAddress").text ## misschien eerder een if statement voor maken voor checken?

        update_fields = {}
        if root.find("FirstName") is not None:
            update_fields['first_name'] = root.find("FirstName").text
        if root.find("LastName") is not None:
            update_fields['last_name'] = root.find("LastName").text
        if root.find("PhoneNumber") is not None:
            update_fields['phone'] = root.find("PhoneNumber").text

        # Business info
        business = root.find("Business")
        if business is not None:
            if business.find("BusinessName") is not None:
                update_fields['company'] = business.find("BusinessName").text
            if business.find("BTWNumber") is not None:
                update_fields['company_vat'] = business.find("BTWNumber").text

        if update_fields:
            set_clause = ", ".join([f"{k} = %s" for k in update_fields])
            values = list(update_fields.values()) + [email]
            
            cursor.execute(
                f"UPDATE client SET {set_clause} WHERE email = %s",
                values
            )
            conn.commit()
            logger.info(f"Updated user {email} with fields: {list(update_fields.keys())}")

            cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error processing user update: {e}")
        return False
    
def on_message(channel, method, properties, body):
    try:
        xml_data = body.decode()
        email = ET.fromstring(xml_data).find('Email').text
        update_client(email, xml_data)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Message processing failed: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

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
        for queue in ['facturatie_user_update', 'crm_user_update', 'kassa_user_update', 'frontend_user_update']:
            channel.queue_declare(queue=queue, durable=True)
            channel.basic_consume(queue=queue, on_message_callback=on_message)

        logger.info("Listening for update messages...")
        channel.start_consuming()
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        channel.stop_consuming()
        connection.close()
        logger.info("consumer stopped.")

if __name__ == "_main_":
    start_consumer()