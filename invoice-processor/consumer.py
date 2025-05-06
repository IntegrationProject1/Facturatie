import pika
import os
from dotenv import load_dotenv
from invoice_processor import process_invoice

# .env inladen
load_dotenv()

def start_consumer():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST"),
            port=int(os.getenv("RABBITMQ_PORT")),
            credentials=pika.PlainCredentials(
                username=os.getenv("RABBITMQ_USER"),
                password=os.getenv("RABBITMQ_PASSWORD")
            )
        )
    )

    channel = connection.channel()

    # Zorg dat exchange bestaat (optioneel bij consumer)
    channel.exchange_declare(exchange='billing', exchange_type='topic', durable=True)

    # Unieke, tijdelijke queue aanmaken
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    # Bind de queue aan de exchange met juiste routing key
    channel.queue_bind(exchange='billing', queue=queue_name, routing_key='order.created')

    print("✅ Waiting for messages...")

    def callback(ch, method, properties, body):
        print("📥 Received message from RabbitMQ")
        process_invoice(body.decode())

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == "__main__":
    start_consumer()
