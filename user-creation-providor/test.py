import pika
import json
import mysql.connector
import bcrypt


db = mysql.connector.connect(
    host="http://integrationproject-2425s2-001.westeurope.cloudapp.azure.com",
    user="admin",
    password="Admin123!",
    database="facturatie"
)

cursor = db.cursor()


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()


exchange_name = "mijn_exchange"


queues = {
    "nieuwe_gebruikers": "nieuwe_gebruikers",
    "premium_gebruikers": "premium",
    "admin_accounts": "admin"
}


channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)


for queue_name, routing_key in queues.items():
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)


def process_message(ch, method, properties, body):
    try:
        user_data = json.loads(body)

        email = user_data.get("email")
        naam = user_data.get("naam")
        wachtwoord = user_data.get("wachtwoord")  
        rol = method.routing_key 
        if email and naam and wachtwoord:
           
            hashed_wachtwoord = bcrypt.hashpw(wachtwoord.encode('utf-8'), bcrypt.gensalt())

            
            sql = "INSERT INTO user (email, naam, wachtwoord, rol, status) VALUES (%s, %s, %s, %s, 'active')"
            values = (email, naam, hashed_wachtwoord, rol)
            cursor.execute(sql, values)
            db.commit()

            print(f"Gebruiker {email} toegevoegd als {rol}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Fout bij verwerken van {method.routing_key}: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


for queue_name in queues.keys():
    channel.basic_consume(queue=queue_name, on_message_callback=process_message)

print("Luistert naar berichten op alle queues...")
channel.start_consuming()
