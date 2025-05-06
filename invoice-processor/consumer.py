import pika
from invoice_processor import process_invoice

def callback(ch, method, properties, body):
    try:
        xml_string = body.decode()
        process_invoice(xml_string)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def start_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.exchange_declare(exchange='billing', exchange_type='topic')
    channel.queue_declare(queue='invoice_queue', durable=True)
    channel.queue_bind(exchange='billing', queue='invoice_queue', routing_key='order.created')

    channel.basic_consume(queue='invoice_queue', on_message_callback=callback)

    print("Waiting for messages...")
    channel.start_consuming()

if __name__ == "__main__":
    start_consumer()
