import pika
import json

def send_to_mailing_queue(invoice):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='billing', exchange_type='topic')
    channel.basic_publish(
        exchange='billing',
        routing_key='order.created',
        body=json.dumps(invoice)
    )
    connection.close()