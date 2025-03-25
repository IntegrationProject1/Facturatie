def send_to_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='user_queue')
    channel.basic_publish(exchange='', routing_key='user_queue', body=xml_message)
    print("Gebruiker toegevoegd aan RabbitMQ queue")
    connection.close()