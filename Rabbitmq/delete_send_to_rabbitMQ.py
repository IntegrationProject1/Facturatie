import pika

def send_delete_user_to_rabbitmq(xml_message):
    # Establish a connection to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    # Declare the queue (if it doesn't already exist)
    # Gaat moeten aangepast worden eens dat de queues opgesteld zijn
    channel.queue_declare(queue='user_queue')
    
    # Publish the XML message to the queue
    channel.basic_publish(exchange='', routing_key='user_queue', body=xml_message)
    # Gaat hier ook aanpassing nodig zijn voor de routing key
    
    print("User deletion message sent to RabbitMQ queue")
    
    # Close the connection
    connection.close()

# Example XML message for deleting a user
xml_message = """<?xml version="1.0" encoding="UTF-8"?>
<UserMessage>
    <ActionType>DeleteUser</ActionType>
    <UserID>67890</UserID>
    <TimeOfAction>2025-03-25T15:00:00</TimeOfAction>
</UserMessage>"""

# Example usage
send_delete_user_to_rabbitmq(xml_message)
