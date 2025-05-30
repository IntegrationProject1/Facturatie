import docker
import pika
import xml.etree.ElementTree as ET
import time
import os
import threading
from collections import defaultdict
from time import time

SERVICE_NAME = "Facturatie"
EXCHANGE_NAME = "log_monitoring"
ROUTING_KEY = "controlroom.log.event"

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")

def create_xml_log(status, message, container_name=None):
    log = ET.Element("Log")
    ET.SubElement(log, "ServiceName").text = f"{SERVICE_NAME}::{container_name}" if container_name else SERVICE_NAME
    ET.SubElement(log, "Status").text = status
    ET.SubElement(log, "Message").text = message
    return ET.tostring(log, encoding='utf-8', method='xml')

def publish_log(xml_message):
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        params = pika.ConnectionParameters(RABBITMQ_HOST, RABBITMQ_PORT, '/', credentials)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct', durable=True)
        channel.queue_declare(queue="controlroom.log.event", durable=True)
        channel.queue_bind(exchange=EXCHANGE_NAME, queue="controlroom.log.event", routing_key=ROUTING_KEY)
        channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=ROUTING_KEY, body=xml_message)
        connection.close()
    except Exception as e:
        print("Fout bij verzenden naar RabbitMQ:", e)

last_log_time = defaultdict(lambda: 0)

def monitor_container_logs(container):
    error_keywords = ["error", "err", "fatal", "critical", "exception"]
    warning_keywords = ["warn", "warning", "deprecated"]

    print("Starting log stream for:", container.name)
    try:
        for line in container.logs(stream=True, follow=True):
            log_line = line.decode('utf-8').strip()
            print(f"[RAW] {container.name}:", repr(log_line))

            # Skip empty or generic lines
            normalized = log_line.lower()
            if normalized in ["", "info", "error", "warning", "deprecated"]:
                print(f"[SKIPPED] Empty or generic log from {container.name}: '{log_line}'")
                continue

            # Throttle: 1 log per 5 seconds per container
            now = time()
            if now - last_log_time[container.name] < 5:
                print(f"[THROTTLED] {container.name} log skipped to reduce spam.")
                continue
            last_log_time[container.name] = now

            # Determine log status
            status = "INFO"
            if any(word in normalized for word in error_keywords):
                status = "ERROR"
            elif any(word in normalized for word in warning_keywords):
                status = "WARNING"

            xml_message = create_xml_log(status, f"{container.name}: {log_line}")
            publish_log(xml_message)

    except Exception as e:
        print(f"Error while streaming logs from {container.name}: {e}")
        
def monitor_logs():
    print("Start met log monitoring...")
    client = docker.from_env()
    whitelist = [
        "facturatie_user_providor",
        "facturatie_update_providor",
        "facturatie_deletion_providor",
        "facturatie_creation_consumer",
        "facturatie_update_consumer",
        "facturatie_deletion_consumer",
        "facturatie_invoice_processor",
        "facturatie_app",
        "facturatie_log_monitor",
                ]
    containers = [c for c in client.containers.list() if c.name in whitelist]

    for container in containers:
        thread = threading.Thread(target=monitor_container_logs, args=(container,))
        thread.daemon = True
        thread.start()

if __name__ == "__main__":
    print("Logger wordt gestart...")
    print("Verbinden met RabbitMQ op", RABBITMQ_HOST, ":", RABBITMQ_PORT, "als", RABBITMQ_USER)

    try:
        # Testlogs bij opstart
        test_msg = create_xml_log("INFO", "Test startbericht van log-monitor")
        publish_log(test_msg)
        publish_log(create_xml_log("ERROR", "Test ERROR log van logger"))
        publish_log(create_xml_log("WARNING", "Test WARNING log van logger"))
        print("Testberichten succesvol verzonden\n")
    except Exception as e:
        print("Kan geen verbinding maken met RabbitMQ:", e, "\n")

    # Start monitoring
    while True:
        try:
            monitor_logs()
            while True:
                time.sleep(1)  # Hou de main-thread levend
        except Exception as e:
            error_log = create_xml_log("ERROR", f"Logger crashed: {str(e)}")
            print("Logger crashed:", e)
            try:
                publish_log(error_log)
            except Exception as pub_error:
                print("Fout bij verzenden van crash-log:", pub_error)
            time.sleep(5)
