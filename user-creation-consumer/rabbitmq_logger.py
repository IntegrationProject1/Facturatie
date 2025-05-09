import logging
import os
import pika
import xml.etree.ElementTree as ET
import datetime

RABBITMQ_EXCHANGE = os.environ.get('RABBITMQ_LOG_EXCHANGE', 'log_monitoring')
RABBITMQ_QUEUE = os.environ.get('RABBITMQ_LOG_QUEUE', 'controlroom.log.events')
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')

def create_log_message(service_name, status, code, message):
    root = ET.Element("Log")
    ET.SubElement(root, "ServiceName").text = service_name
    ET.SubElement(root, "Status").text = status
    ET.SubElement(root, "Code").text = code
    ET.SubElement(root, "Message").text = message
    return ET.tostring(root, encoding="utf-8", method="xml")

def send_log(service_name, status, code, message):
    xml_data = create_log_message(service_name, status, code, message)
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
        )
        channel = connection.channel()
        channel.exchange_declare(exchange=RABBITMQ_EXCHANGE, exchange_type='direct', durable=True)
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        channel.queue_bind(exchange=RABBITMQ_EXCHANGE, queue=RABBITMQ_QUEUE, routing_key='logs.events')
        channel.basic_publish(
            exchange=RABBITMQ_EXCHANGE,
            routing_key='logs.events',
            body=xml_data,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type='application/xml'
            )
        )
        connection.close()
    except Exception as e:
        print(f"[LogError] Kon log niet verzenden: {e}")
