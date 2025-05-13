import subprocess
import pika
import time
import xml.etree.ElementTree as ET

# RabbitMQ Config
RABBITMQ_HOST = "integrationproject-2425s2-001.westeurope.cloudapp.azure.com"
RABBITMQ_PORT = 30020
RABBITMQ_USER = "ehbstudent"
RABBITMQ_PASSWORD = "wpqjf9mI3DKZdZDaa!"
ROUTING_KEY = "controlroom.log.event"
EXCHANGE = "log_monitoring"
SERVICE_NAME = "Facturatie"

def send_log_to_rabbitmq(status, message):
    root = ET.Element("Log")
    ET.SubElement(root, "ServiceName").text = SERVICE_NAME
    ET.SubElement(root, "Status").text = status
    ET.SubElement(root, "Message").text = message
    xml_data = ET.tostring(root, encoding='utf-8')

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct', durable=True)
    channel.basic_publish(exchange=EXCHANGE, routing_key=ROUTING_KEY, body=xml_data)
    connection.close()

def get_logs_from_container(name):
    try:
        logs = subprocess.check_output(["docker", "logs", "--tail", "5", name], stderr=subprocess.STDOUT)
        return logs.decode("utf-8")
    except subprocess.CalledProcessError as e:
        return f"[ERROR] Kan logs niet ophalen van {name}: {e.output.decode('utf-8')}"

def main():
    containers = [
        "facturatie_user_providor",
        "facturatie_update_providor",
        "facturatie_deletion_providor",
        "facturatie_creation_consumer",
        "facturatie_update_consumer",
        "facturatie_deletion_consumer",
        "facturatie_heartbeat"
    ]
    logs_sent = {}

    while True:
        for name in containers:
            logs = get_logs_from_container(name)
            if logs and logs != logs_sent.get(name):
                send_log_to_rabbitmq("INFO", f"[{name}] {logs.strip()}")
                logs_sent[name] = logs
        time.sleep(10)

if __name__ == "__main__":
    main()
