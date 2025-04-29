import pika
import os
import logging
import xml.etree.ElementTree as ET
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
 
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
 
# Load environment variables and check if user exists
def user_exists(uuid_timestamp):
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor()
 
    try:
        cursor.execute("SELECT id FROM client WHERE created_at = %s", (uuid_timestamp,))
        return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"Error checking user existence: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
 
# Delete user from FossBilling database
def delete_user(user_data):
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor()
 
    # check if client exists
    # if we do not check if the client exists, we won't get an error on deletion
    # because technically nothing went wrong, the client just didn't exist
    try:
        uuid_timestamp = user_data['uuid']
 
        # check if user exists
        if not user_exists(uuid_timestamp):
            logger.warning(f"Client with created_at {uuid_timestamp} not found - nothing to delete")
            return False
 
        # so now i only delete the client if it exists -> helps with debugging if something goes wrong as well
        cursor.execute("DELETE FROM client WHERE created_at = %s", (uuid_timestamp,))
        conn.commit()
        logger.info(f"Deleted client: {uuid_timestamp}")
        return True
 
    except Exception as e:
        logger.error(f"Deletion failed: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
 
# Parse XML message
def parse_user_xml(xml_data):
    try:
        root = ET.fromstring(xml_data)
 
        return {
            'action_type': root.find('ActionType').text,
            'uuid': root.find('UUID').text,
            'action_time': root.find('TimeOfAction').text
        }
    except Exception as e:
        logger.error(f"XML parsing failed: {e}")
        raise
 
# Callback for when a message is received from RabbitMQ
def on_message(channel, method, properties, body):
    # channel: the channel that received the message
    # method: information about the message (message metadata)
    # properties: message properties (headers, priority, ....)
    # body: the message itself
    try:
        logger.info(f"Received message from {method.routing_key}")
 
        # Parse XML
        user_data = parse_user_xml(body.decode())
 
        # Only process DELETE actions
        if user_data['action_type'].upper() != 'DELETE':
            logger.warning(f"Ignoring non-DELETE action: {user_data['action_type']}")
            channel.basic_ack(method.delivery_tag)
            return
 
        # Clean UUID timestamp (remove 'Z' if present)
        if user_data['uuid'].endswith('Z'):
            user_data['uuid'] = user_data['uuid'][:-1]
 
        # Delete user from database
        delete_user(user_data)
 
        # Acknowledge message
        channel.basic_ack(method.delivery_tag)
        # 'tells' RabbitMQ that the message was processed successfully
        # the message is then removed from the queue
 
    except Exception as e:
        logger.error(f"Message processing failed: {e}")
        channel.basic_nack(method.delivery_tag, requeue=False)
        # 'tells' RabbitMQ that the message processing failed
        # requeue=False -> the message is not requeued, but discarded or sent to a dead-letter queue
 
# Start the RabbitMQ consumer
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
 
    try:
        # Declare all queues we want to listen to
        queues = ['facturatie_user_delete']
        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)
            channel.basic_consume(
                queue=queue,
                on_message_callback=on_message,
                auto_ack=False
            )
            # queue=queue -> this is the queue we are listening to
            # durable=True -> the queue will survive a RabbitMQ server restart
            # the whole function ensures that the queues exist and are ready to receive messages
            # whenever a new message is received, the on_message_callback function is called
 
        logger.info("Waiting for user deletion messages...")
        channel.start_consuming()
 
        # you can interrupt the consumer with CTRL+C
        # this just makes sure that the connection is closed properly
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
        channel.stop_consuming()
        connection.close()
        logger.info("Consumer stopped.")
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        raise
 
if __name__ == "__main__":
    start_consumer()