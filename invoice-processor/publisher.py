import pika
import os
import json
from dotenv import load_dotenv

load_dotenv()

def publish_invoice(invoice_id):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST"),
            port=int(os.getenv("RABBITMQ_PORT")),
            credentials=pika.PlainCredentials(
                os.getenv("RABBITMQ_USER"),
                os.getenv("RABBITMQ_PASSWORD")
            )
        )
    )

    channel = connection.channel()
    channel.exchange_declare(exchange='mailing', exchange_type='fanout', durable=True)

    message = json.dumps({"invoice_id": invoice_id})
    channel.basic_publish(exchange='mailing', routing_key='', body=message)
    print(f"📤 Sent invoice {invoice_id} to mailing queue")

    connection.close()
