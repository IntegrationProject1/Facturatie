import os
import pika
import xml.etree.ElementTree as ET

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')
RABBITMQ_LOG_EXCHANGE = os.environ.get('RABBITMQ_LOG_EXCHANGE', 'log_monitoring')
RABBITMQ_LOG_ROUTING_KEY = os.environ.get('RABBITMQ_LOG_ROUTING_KEY', 'controlroom.log.events')

def send_log(service_name, status, code, message):
    try:
        # Bouw XML volgens LOG_XSD
        root = ET.Element("Log")
        ET.SubElement(root, "ServiceName").text = service_name
        ET.SubElement(root, "Status").text = status
        ET.SubElement(root, "Code").text = str(code)
        ET.SubElement(root, "Message").text = message
        xml_str = ET.tostring(root, encoding="utf-8")

        # Verbind met RabbitMQ
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials
        ))
        channel = connection.channel()
        channel.exchange_declare(exchange=RABBITMQ_LOG_EXCHANGE, exchange_type='direct', durable=True)
        channel.basic_publish(
            exchange=RABBITMQ_LOG_EXCHANGE,
            routing_key=RABBITMQ_LOG_ROUTING_KEY,
            body=xml_str
        )
        connection.close()
    except Exception as e:
        print(f"[LOGGER ERROR] Kan log niet versturen: {e}")
