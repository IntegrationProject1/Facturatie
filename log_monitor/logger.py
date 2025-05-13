import docker
import pika
import time
import xml.etree.ElementTree as ET

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
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    )
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type='direct', durable=True)
    channel.basic_publish(exchange=EXCHANGE, routing_key=ROUTING_KEY, body=xml_data)
    connection.close()

def main():
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')

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
            try:
                container = client.containers.get(name)
                logs = container.logs(tail=5).decode("utf-8")
                if logs and logs != logs_sent.get(name):
                    send_log_to_rabbitmq("INFO", f"[{name}] {logs.strip()}")
                    logs_sent[name] = logs
            except Exception as e:
                send_log_to_rabbitmq("ERROR", f"Fout bij ophalen logs van {name}: {str(e)}")
        time.sleep(10)

if __name__ == "__main__":
    main()
