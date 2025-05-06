import pika
import os
import logging
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# RabbitMQ verbinding opzetten
def start_log_consumer():
    credentials = pika.PlainCredentials(
        os.environ["RABBITMQ_USER"],
        os.environ["RABBITMQ_PASSWORD"]
    )
    parameters = pika.ConnectionParameters(
        host=os.environ["RABBITMQ_HOST"],
        port=int(os.environ["RABBITMQ_PORT"]),
        credentials=credentials
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Declare exchange en queue
    channel.exchange_declare(exchange='log_monitoring', exchange_type='fanout', durable=True)
    channel.queue_declare(queue='controlroom.log.events', durable=True)
    channel.queue_bind(exchange='log_monitoring', queue='controlroom.log.events')

    logger.info("Log consumer actief op queue: controlroom.log.events")

    # Callback functie om berichten op te vangen
    def callback(ch, method, properties, body):
        logger.info("Log ontvangen:\n%s", body.decode())

    channel.basic_consume(queue='controlroom.log.events', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == "__main__":
    start_log_consumer()
