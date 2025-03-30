import pika
import xml.etree.ElementTree as ET
import logging
import mysql.connector
import os
import time
from dotenv import load_dotenv

#reads the .env file
load_dotenv()

# set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Function to get the text of an XML element. it also prevents the code from crashing if the element is not found
def get_xml_text(parent, tag):
    elem = parent.find(tag)
    return elem.text if elem is not None else None

# Function to update the user in the database. It checks if the user exists by email and then updates the fields
def update_client(email, root):
    # Connect to the database
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor()
    # find the user by email
    cursor.execute("SELECT id FROM client WHERE email = %s", (email,))
    user = cursor.fetchone()
    
    # update the fields gather new information from the xml
    try:
        update_fields = {}

        if get_xml_text(root, "FirstName"):
            update_fields['first_name'] = get_xml_text(root, "FirstName")
        if get_xml_text(root, "LastName"):
            update_fields['last_name'] = get_xml_text(root, "LastName")
        if get_xml_text(root, "PhoneNumber"):
            update_fields['phone'] = get_xml_text(root, "PhoneNumber")

        business = root.find("Business")
        if business is not None:
            if get_xml_text(business, "BusinessName"):
                update_fields['company'] = get_xml_text(business, "BusinessName")
            if get_xml_text(business, "BTWNumber"):
                update_fields['company_vat'] = get_xml_text(business, "BTWNumber")

            # build and run the update query. It updates the fields that have been changed
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
# Function to process the message. It reads the xml and gets the email.
def on_message(channel, method, properties, body):
    try:
        #decode the xml and get the user email
        xml_data = body.decode()
        root = ET.fromstring(xml_data)
        email = get_xml_text(root, 'EmailAddress')
        # if the email is missing, skip the message
        if not email:
            logger.warning("EmailAddress not found in message. Skipping.")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        update_client(email, root)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error(f"Message processing failed: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
# Function to start the consumer. It connects to the rabbitmq server and listens for messages
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
# it listens for messages from the following queues and when a message is received it calls the on_message function
    for queue in ['facturatie_user_update', 'crm_user_update', 'kassa_user_update', 'frontend_user_update']:
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_consume(queue=queue, on_message_callback=on_message)

    logger.info("Listening for update messages...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        channel.stop_consuming()
        connection.close()
        logger.info("consumer stopped.")

if __name__ == "__main__":
    start_consumer()
