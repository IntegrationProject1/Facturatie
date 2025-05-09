import pika
import os
import xml.etree.ElementTree as ET
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_log(service_name, status, code, message):
    try:
        # XML opbouwen
        log_xml = ET.Element("Log")
        ET.SubElement(log_xml, "ServiceName").text = service_name
        ET.SubElement(log_xml, "Status").text = status
        ET.SubElement(log_xml, "Code").text = str(code)
        ET.SubElement(log_xml, "Message").text = message

        xml_string = '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(log_xml, encoding='unicode')

        # RabbitMQ connectie
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

        # Declareer log exchange (fanout)
        exchange_name = os.environ.get("RABBITMQ_LOG_EXCHANGE", "log_monitoring")
        channel.exchange_declare(exchange=exchange_name, exchange_type="direct", durable=True)

        # Publiceer bericht
        channel.basic_publish(
            exchange=exchange_name,
            routing_key="logs.events",
            body=xml_string,
            properties=pika.BasicProperties(delivery_mode=2)
        )

        connection.close()
        logger.info("Log message sent to log_monitoring")
    except Exception as e:
        logger.error(f"Failed to send log message: {e}")
