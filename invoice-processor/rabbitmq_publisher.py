import pika
import json
import os

def send_to_mailing_queue(invoice):
    try:
        # Load RabbitMQ connection parameters from environment variables
        rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
        rabbitmq_port = int(os.getenv("RABBITMQ_PORT", 5672))
        rabbitmq_user = os.getenv("RABBITMQ_USER", "guest")
        rabbitmq_password = os.getenv("RABBITMQ_PASSWORD", "guest")
        
        # Set up RabbitMQ connection
        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=credentials)
        )
        channel = connection.channel()
        
        # Declare the exchange
        channel.queue_declare(queue="mail_queue", durable=True)
        
        # Publish the message
        channel.basic_publish(
            exchange='',
            routing_key='mail_queue',
            body=json.dumps(invoice),
            properties=pika.BasicProperties(
                delivery_mode=2  # Make message persistent
            )
        )
        print(f"Invoice {invoice['id']} sent to mailing queue.")
    except Exception as e:
        print(f"Failed to send invoice to mailing queue: {e}")
    finally:
        # Ensure the connection is closed
        if 'connection' in locals() and connection.is_open:
            connection.close()