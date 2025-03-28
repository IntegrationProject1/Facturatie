import pika
import os
import time
import logging
import xml.etree.ElementTree as ET # for creating XML messages
from datetime import datetime, timezone
from dotenv import load_dotenv # so pyhton can read the .env file

# loading env variables from .env file
load_dotenv()

# define logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# RabbitMQ connection setup with credentials from .env file
def get_rabbitmq_connection():
    try:
        params = pika.ConnectionParameters( # connection parameters
            host=os.environ["RABBITMQ_HOST"],
            port=int(os.environ["RABBITMQ_PORT"]),
            virtual_host="/",
            credentials=pika.PlainCredentials(
                os.environ["RABBITMQ_USER"],
                os.environ["RABBITMQ_PASSWORD"]
            ),
            heartbeat=600, # heartbeat interval 
            blocked_connection_timeout=300 
        )
        return pika.BlockingConnection(params)
    except Exception as e: 
        logger.error(f"Fout bij verbinden met RabbitMQ: {e}") # log error when connection fails
        return None

# XML heartbeat-message 
def create_heartbeat_message():
    root = ET.Element("IncomingFacturationMessage")
    ET.SubElement(root, "InvoiceID").text = "HEARTBEAT"  # Dummy data to identify heartbeat
    ET.SubElement(root, "CustomerID").text = "SYSTEM"
    ET.SubElement(root, "TotalAmount").text = "0.00"  # no actual amount for heartbeat
    ET.SubElement(root, "DueDate").text = datetime.now(timezone.utc).strftime("%Y-%m-%d")  # current date as placeholder

    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding='unicode')

# Heartbeat to rabbitmq exchange
def send_heartbeat():
    connection = get_rabbitmq_connection()

    if connection:
        try:
            channel = connection.channel()
            channel.exchange_declare(exchange="monitoring", exchange_type="fanout", durable=True) 

            heartbeat_xml = create_heartbeat_message() # create heartbeat message
            channel.basic_publish(
                exchange="monitoring",
                routing_key="",
                body=heartbeat_xml
            )

            logger.info("Heartbeat verzonden naar ControlRoom") # log message when heartbeat is sent
            connection.close()
        except Exception as e:
            logger.error(f"Fout bij verzenden van heartbeat: {e}")
    else:
        logger.error("Kan geen verbinding maken met RabbitMQ!")

# Main loop: each minute send a heartbeat
if __name__ == "__main__":
    logger.info("Heartbeat sender gestart")
    while True:
        send_heartbeat()
        time.sleep(60) # time after which heartbeat is sent again
