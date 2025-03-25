import pika

def send_update_user_to_rabbitmq(xml_message):
    # Establish a connection to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    # Declare the queue (if it doesn't already exist)
    # Gaat moeten aangepast worden eens dat de queues opgesteld zijn
    channel.queue_declare(queue='user_queue')
    
    # Publish the XML message to the queue
    # Gaat hier ook aanpassing nodig zijn voor de routing key
    channel.basic_publish(exchange='', routing_key='user_queue', body=xml_message)
    
    print("User update message sent to RabbitMQ queue")
    
    # Close the connection
    connection.close()

# Example XML message for updating a user
xml_message = """<?xml version="1.0" encoding="UTF-8"?>
<UserMessage>
    <ActionType>UpdateUser</ActionType>
    <UserID>67890</UserID>
    <TimeOfAction>2025-03-25T15:00:00</TimeOfAction>
    <FirstName>Amine</FirstName>
    <LastName>Zerouali</LastName>
    <PhoneNumber>+32456789012</PhoneNumber>
    <EmailAddress>amine@example.com</EmailAddress>
    <Business>
        <BusinessName>AZ Web Solutions</BusinessName>
        <BusinessEmail>contact@azwebsolutions.com</BusinessEmail>
        <RealAddress>Brussels, Belgium</RealAddress>
        <BTWNumber>BE123456789</BTWNumber>
        <FacturationAddress>Brussels, Belgium</FacturationAddress>
    </Business>
</UserMessage>"""

# Example usage
send_update_user_to_rabbitmq(xml_message)
