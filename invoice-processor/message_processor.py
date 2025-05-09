# message_processor.py
import pika
import os
import xml.etree.ElementTree as ET
from datetime import datetime
import logging
import mysql.connector
from fossbilling import generate_invoice, send_to_mailing_queue

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# RabbitMQ settings
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
RABBITMQ_PORT = os.environ["RABBITMQ_PORT"]
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASSWORD = os.environ["RABBITMQ_PASSWORD"]

# MySQL database connection
def get_db_connection():
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"]
    )

# Process incoming messages
def process_message(ch, method, properties, body):
    logger.info("Received a message")
    try:
        # Parse the incoming XML
        root = ET.fromstring(body)
        order_data = extract_order_data(root)
        
        # Create invoice in FossBilling system
        invoice = generate_invoice(order_data)
        
        # Send the invoice to the mailing queue
        send_to_mailing_queue(invoice)
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"Processed order {order_data['UUID']} successfully")

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag)

# Extract order data from the XML
def extract_order_data(root):
    order_data = {}
    order_data['date'] = root.find('Date').text
    order_data['UUID'] = root.find('UUID').text
    
    # Extract products
    products = []
    for product_element in root.findall(".//Product"):
        product = {
            'ProductNR': product_element.find('ProductNR').text,
            'Quantity': product_element.find('Quantity').text,
            'UnitPrice': product_element.find('UnitPrice').text
        }
        products.append(product)
    
    order_data['Products'] = products
    return order_data

# Setup RabbitMQ connection and start listening for messages
def start_listening():
    try:
        # Establish connection to RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=int(RABBITMQ_PORT),
            credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        ))
        channel = connection.channel()

        # Declare the queue to consume from
        channel.queue_declare(queue="order.created", durable=True)
        
        # Start consuming messages
        channel.basic_consume(queue="order.created", on_message_callback=process_message)

        logger.info("Listening for messages...")
        channel.start_consuming()

    except Exception as e:
        logger.error(f"Error in RabbitMQ connection: {e}")

if __name__ == "__main__":
    start_listening()
