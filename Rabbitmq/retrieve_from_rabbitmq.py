import pika

# Ophalen uit RabbitMQ
def retrieve_from_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='user_queue')

    def callback(ch, method, properties, body):
        print("Ontvangen uit RabbitMQ:", body.decode())

    channel.basic_consume(queue='user_queue', on_message_callback=callback, auto_ack=True)
    print("Wachten op berichten uit RabbitMQ...")
    channel.start_consuming()

if __name__ == "__main__":
    retrieve_from_rabbitmq()
