import pika, os, logging
import xml.etree.ElementTree as ET
import mysql.connector

# Sets up logging to display timestamps, log levels, and messages.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to delete a client from the database based on their email.
def delete_client(email):

    #database connection using environment variables for credentials
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor()

    # SQL query to delete a client from the database
    try:
        cursor.execute("DELETE FROM client WHERE email = %s", (email,))
        conn.commit()
        logger.info(f"Deleted client: {email}")
    except Exception as e:  # Handles any exceptions that occur during the deletion process
        logger.error(f"Deletion failed: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Function to handle incoming messages from RabbitMQ
def on_message(channel, method, properties, body):
    try:
        xml_data = body.decode()
        email = ET.fromstring(xml_data).find('Email').text
        delete_client(email)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Message processing failed: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

# Function to set up the RabbitMQ consumer
def start_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST"),
        port=int(os.getenv("RABBITMQ_PORT")),
        credentials=pika.PlainCredentials(
            os.getenv("RABBITMQ_USER"),
            os.getenv("RABBITMQ_PASSWORD")
        )
    ))
    channel = connection.channel()
    
    # Declare the queues to ensure they exist before consuming messages
    for queue in ['kassa_user_delete', 'crm_user_delete', 'frontend_user_delete']:
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_consume(queue=queue, on_message_callback=on_message)
    
    logger.info("Listening for deletion messages...")
    channel.start_consuming()

# Main function to start the consumer
if __name__ == "__main__":
    start_consumer()