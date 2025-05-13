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
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        params = pika.ConnectionParameters(RABBITMQ_HOST, RABBITMQ_PORT, '/', credentials)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct', durable=True)
        channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=ROUTING_KEY, body=xml_message)
        connection.close()
        print("Log verzonden naar RabbitMQ")
    except Exception as e:
        print("Fout bij verzenden naar RabbitMQ:", e)

def monitor_logs():
    print("Start met log monitoring...")
    client = docker.from_env()
    containers = client.containers.list()

    for container in containers:
        print("Container gevonden:", container.name)
        try:
            for line in container.logs(stream=True, follow=True, tail=10):
                log_line = line.decode('utf-8').strip()
                print("Log uit", container.name + ":", log_line)

                status = "INFO"
                if "error" in log_line.lower():
                    status = "ERROR"
                elif "warn" in log_line.lower():
                    status = "WARNING"

                xml_message = create_xml_log(status, f"{container.name}: {log_line}")
                publish_log(xml_message)
        except Exception as e:
            print("Fout bij lezen van logs voor", container.name + ":", e)

if __name__ == "__main__":
    print("Logger wordt gestart...")
    print("Verbinden met RabbitMQ op", RABBITMQ_HOST + ":", RABBITMQ_PORT, "als", RABBITMQ_USER)
    try:
        test_msg = create_xml_log("INFO", "Test startbericht van log-monitor")
        publish_log(test_msg)
        print("Testbericht succesvol verzonden\n")
    except Exception as e:
        print("Kan geen verbinding maken met RabbitMQ:", e, "\n")

    while True:
        try:
            monitor_logs()
        except Exception as e:
            error_log = create_xml_log("CRITICAL", f"Logger crashed: {str(e)}")
            print("Logger crashed:", e)
            try:
                publish_log(error_log)
            except Exception as pub_error:
                print("Fout bij verzenden van crash-log:", pub_error)
            time.sleep(5)
