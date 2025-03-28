import pika
import os
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime

# Instellen van logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# RabbitMQ verbinding instellen
def get_rabbitmq_connection():
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
        return pika.BlockingConnection(params)
    except Exception as e:
        logger.error(f"Fout bij verbinden met RabbitMQ: {e}")
        return None

# XML heartbeat-bericht maken
def create_heartbeat_message():
    root = ET.Element("HeartbeatMessage")
    
    ET.SubElement(root, "ServiceName").text = "Facturatie"
    ET.SubElement(root, "Status").text = "Online"
    ET.SubElement(root, "Timestamp").text = datetime.utcnow().isoformat() + "Z"

    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding='unicode')

# Heartbeat naar RabbitMQ sturen
def send_heartbeat():
    connection = get_rabbitmq_connection()
    
    if connection:
        try:
            channel = connection.channel()
            channel.queue_declare(queue="controlroom_heartbeat", durable=True)
            
            heartbeat_xml = create_heartbeat_message()
            channel.basic_publish(
                exchange="monitoring",
                routing_key="controlroom.heartbeat",
                body=heartbeat_xml
            )

            logger.info("Heartbeat verzonden naar ControlRoom")
            connection.close()
        except Exception as e:
            logger.error(f"Fout bij verzenden van heartbeat: {e}")

# Main loop om elke 60s een heartbeat te versturen
if __name__ == "__main__":
    logger.info("Heartbeat sender gestart")
    while True:
        send_heartbeat()
        time.sleep(60)  # Wacht 60 seconden en stuur opnieuw
