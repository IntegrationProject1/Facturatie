import pika 
import os
from dotenv import load_dotenv 

def send_to_rabbitmq():
    xml_message = "<message>Sample XML content</message>"  # Define the xml_message variable
    connection = pika.BlockingConnection(pika.ConnectionParameters(ip_address = os.getenv("IP_ADDRESS")))
    channel = connection.channel()
    channel.basic_publish(exchange='', routing_key='facturatie.user.create', body=xml_message)
    print("Gebruiker toegevoegd aan RabbitMQ queue")
    connection.close()