import docker
import pika
import xml.etree.ElementTree as ET
import time
import os

SERVICE_NAME = "Facturatie"
EXCHANGE_NAME = "log_monitoring"
ROUTING_KEY = "controlroom.log.event"

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")

def create_xml_log(status, message):
    log = ET.Element("Log")
    ET.SubElement(log, "ServiceName").text = SERVICE_NAME
    ET.SubElement(log, "Status").text = status
    ET.SubElement(log, "Message").text = message
    return ET.tostring(log, encoding='utf-8', method='xml')

def publish_log(xml_message):
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(RABBITMQ_HOST, RABBITMQ_PORT, '/', credentials)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct', durable=True)
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=ROUTING_KEY, body=xml_message)
    connection.close()

def monitor_logs():
    client = docker.from_env()
    containers = client.containers.list()

    for container in containers:
        for line in container.logs(stream=True, follow=True, tail=0):
            log_line = line.decode('utf-8').strip()
            status = "INFO"
            if "error" in log_line.lower():
                status = "ERROR"
            elif "warn" in log_line.lower():
                status = "WARNING"
            xml_message = create_xml_log(status, f"{container.name}: {log_line}")
            publish_log(xml_message)

if __name__ == "__main__":
    while True:
        try:
            monitor_logs()
        except Exception as e:
            error_log = create_xml_log("CRITICAL", f"Logger crashed: {str(e)}")
            try:
                publish_log(error_log)
            except:
                pass
            time.sleep(5)
