import pika  

def send_to_rabbitmq():
    xml_message = "<message>Sample XML content</message>"  # Define the xml_message variable
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='user_queue')
    channel.basic_publish(exchange='', routing_key='facturatie_user_create', body=xml_message)
    print("Gebruiker toegevoegd aan RabbitMQ queue")
    connection.close()