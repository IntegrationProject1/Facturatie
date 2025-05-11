import pika
import os
import xml.etree.ElementTree as ET
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_log(service_name, status, message):
    try:
        log_xml = ET.Element("Log")
        ET.SubElement(log_xml, "ServiceName").text = service_name
        ET.SubElement(log_xml, "Status").text = status
        ET.SubElement(log_xml, "Message").text = message

        xml_string = '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(log_xml, encoding='unicode')

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

        exchange_name = os.environ.get("RABBITMQ_LOG_EXCHANGE", "log_monitoring")
        routing_key = os.environ.get("RABBITMQ_LOG_ROUTING_KEY", "controlroom.log.event")

        channel.queue_declare(queue="controlroom.log.event", durable=True)
        channel.exchange_declare(exchange=exchange_name, exchange_type="direct", durable=True)
        channel.queue_bind(exchange=exchange_name, queue="controlroom.log.event", routing_key=routing_key)

        channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=xml_string,
            properties=pika.BasicProperties(delivery_mode=2)
        )

        connection.close()
        logger.info("Log message sent to controlroom.log.event")

    except Exception as e:
        logger.error(f"Failed to send log message: {e}")
