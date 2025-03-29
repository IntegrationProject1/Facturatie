import pika
import os
import mysql.connector
import xml.etree.ElementTree as ET

# RabbitMQ connection settings from .env file
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "integrationproject-2425s2-001.westeurope.cloudapp.azure.com")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 30020))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "ehbstudent")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "wpqjf9mI3DKZdZDaa!")

# MySQL connection settings
DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "facturatie")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Admin123!")

# Function to insert a new user into the database
def insert_user(email, name):
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
   
    )
    cursor = conn.cursor()
    
    try:
        # Check if the user already exists
        cursor.execute("SELECT id FROM client WHERE email = %s", (email,))
        if cursor.fetchone():
            print(f"User {email} already exists in the database.")
        else:
            cursor.execute("INSERT INTO client (email, name) VALUES (%s, %s)", (email, name))
            conn.commit()
            print(f"Inserted new user: {email}, {name}")
    except Exception as e:
        print(f"Failed to insert user: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Create a connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=RABBITMQ_PORT,
    credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
))
channel = connection.channel()

# Define the queues to listen to
queues = ["crm_user_create", "frontend_user_create", "kassa_user_create","facturatie_user_create"]

# Declare the queues in case they don’t exist
for queue in queues:
    channel.queue_declare(queue=queue, durable=True)

# Create a callback function to process messages
def on_message(channel, method, properties, body):
    try:
        xml_data = body.decode()
        root = ET.fromstring(xml_data)
        email = root.find('Email').text
        name = root.find('Name').text
        
        insert_user(email, name)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Failed to process message: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

# Start consuming messages from each queue
for queue in queues:
    channel.basic_consume(queue=queue, on_message_callback=on_message)

print("Listening for new user creation messages...")
channel.start_consuming()
