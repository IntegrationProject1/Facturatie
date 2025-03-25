import pika
import os
from dotenv import load_dotenv  

# Load env variables
load_dotenv()

# Path to the xml file of create_user
xml_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "create_user.xml"))

# Read the XML message from the file
try:
    with open(xml_file_path, "r", encoding="utf-8") as file:
        xml_message = file.read()
except FileNotFoundError:
    print(f"Error: The file '{xml_file_path}' was not found.")
    exit(1)

# Setup connection parameters
parameters = pika.ConnectionParameters(
    host=os.getenv("RABBITMQ_HOST"),
    port=int(os.getenv("RABBITMQ_PORT")),
    virtual_host="/",
    credentials=pika.PlainCredentials(os.getenv("RABBITMQ_USER"),os.getenv("RABBITMQ_PASS")),
    heartbeat=600,
    blocked_connection_timeout=300
)

# Establish connection and send message
try:
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    # queue_name not routing key!
    queue_name = "facturatie_user_create"
    channel.queue_declare(queue=queue_name, durable=True)

    # exchange_name not queue_name!
    exchange_name = "user"

    # routing_key not queue_name!
    routing_key = "facturatie.user.create"

    channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=xml_message) # body is the message create_user.xml

    # Confirming the message was sent
    print(f"Message sent successfully to queue '{queue_name}'.")

    connection.close()

# error handling
except pika.exceptions.AMQPConnectionError as e:
    print(f"Failed to connect to RabbitMQ: {e}")
    exit(1)