import pika
import json
import os
from dotenv import load_dotenv

load_dotenv()

def send_to_mailing(invoice_data):
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER"), os.getenv("RABBITMQ_PASSWORD")
    )
    parameters = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST"),
        port=int(os.getenv("RABBITMQ_PORT")),
        credentials=credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.exchange_declare(exchange='mailing', exchange_type='fanout')

    message = json.dumps(invoice_data)
    channel.basic_publish(exchange='mailing', routing_key='', body=message)
    connection.close()
