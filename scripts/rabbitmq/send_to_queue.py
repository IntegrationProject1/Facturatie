import pika
import os

def send_to_queue(xml_file_path):
    with open(xml_file_path, "r", encoding="utf-8") as file:
        xml_message = file.read()

    parameters = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST"),
        port=int(os.getenv("RABBITMQ_PORT")),
        virtual_host="/",
        credentials=pika.PlainCredentials(os.getenv("RABBITMQ_USER"), os.getenv("RABBITMQ_PASS")),
        heartbeat=600,
        blocked_connection_timeout=300
    )

    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        exchange_name = "user"
        routing_key = "facturatie.user.create"

        channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=xml_message)
        print(f"Message sent successfully to queue.")
        connection.close()
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Failed to connect to RabbitMQ: {e}")
