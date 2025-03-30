import pika
import os
import mysql.connector
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# RabbitMQ connection settings from .env file
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")

# MySQL connection settings
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

# Function to insert a new user into the database
def insert_user(email, first_name):
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
                connection_timeout=600,  
    read_timeout=600 
            

        )
        conn.autocommit = True 
        cursor = conn.cursor()

        # Debugging output
        print(f"Attempting to insert user: {email}, {first_name}")

        # Check if the user already exists
        cursor.execute("SELECT id FROM client WHERE email = %s", (email,))
        if cursor.fetchone():
            print(f"User {email} already exists in the database.")
        else:
            cursor.execute("INSERT INTO client (email, first_name) VALUES (%s, %s)", (email, first_name))
            conn.commit()
            print(f"Inserted new user: {email}, {first_name}")
    except mysql.connector.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"Unexpected error: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Create a connection to RabbitMQ
try:
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    ))
    channel = connection.channel()
    print("Connected to RabbitMQ")
except Exception as e:
    print(f"Failed to connect to RabbitMQ: {e}")
    exit(1)

# Define the queues to listen to
queues = ["crm_user_create", "frontend_user_create", "kassa_user_create", "facturatie_user_create"]

# Declare the queues in case they don't exist
for queue in queues:
    channel.queue_declare(queue=queue, durable=True)
    print(f"Listening to queue: {queue}")

# Create a callback function to process messages
def on_message(channel, method, properties, body):
    try:
        xml_data = body.decode()
        print(f"Received message: {xml_data}")  # Debugging output
        
        # Parse XML
        root = ET.fromstring(xml_data)
        
        # Extract required fields from XML
        action_type = root.find('ActionType').text if root.find('ActionType') is not None else None
        user_id = root.find('UserID').text if root.find('UserID') is not None else None
        time_of_action = root.find('TimeOfAction').text if root.find('TimeOfAction') is not None else None
        first_name = root.find('FirstName').text if root.find('FirstName') is not None else None
        last_name = root.find('LastName').text if root.find('LastName') is not None else None
        phone_number = root.find('PhoneNumber').text if root.find('PhoneNumber') is not None else None
        email_address = root.find('EmailAddress').text if root.find('EmailAddress') is not None else None
        
        # Log the extracted values
        print(f"ActionType: {action_type}, UserID: {user_id}, FirstName: {first_name}, LastName: {last_name}, EmailAddress: {email_address}")
        
        # Now insert the data into the database
        if email_address and first_name:  # Ensure we have necessary info to insert
            insert_user(email_address, first_name)
        else:
            print("Error: Missing necessary fields (Email or FirstName) in the XML.")

        # Acknowledge the message
        channel.basic_ack(delivery_tag=method.delivery_tag)
    
    except ET.ParseError as e:
        print(f"XML Parsing error: {e}")
        # Nack the message and don't requeue it
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"Failed to process message: {e}")
        # Nack the message and don't requeue it
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

# Start consuming messages from each queue
for queue in queues:
    channel.basic_consume(queue=queue, on_message_callback=on_message)

print("Listening for new user creation messages...")
channel.start_consuming()