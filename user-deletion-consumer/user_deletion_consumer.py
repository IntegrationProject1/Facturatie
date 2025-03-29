import pika, os, logging
import xml.etree.ElementTree as ET
import mysql.connector

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def delete_client(email):
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM client WHERE email = %s", (email,))
        conn.commit()
        logger.info(f"Deleted client: {email}")
    except Exception as e:
        logger.error(f"Deletion failed: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def on_message(channel, method, properties, body):
    try:
        xml_data = body.decode()
        email = ET.fromstring(xml_data).find('Email').text
        delete_client(email)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Message processing failed: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

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
    
    # Declare all queues
    for queue in ['kassa_user_delete', 'crm_user_delete', 'frontend_user_delete']:
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_consume(queue=queue, on_message_callback=on_message)
    
    logger.info("Listening for deletion messages...")
    channel.start_consuming()

if __name__ == "__main__":
    start_consumer()