import pika, os, logging
import xml.etree.ElementTree as ET
import mysql.connector

# For logging and debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Disable pika logging
pika_logger = logging.getLogger("pika")
pika_logger.handlers.clear()  # Removes any existing handlers
pika_logger.propagate = False
pika_logger.setLevel(logging.WARNING)  # Only show warnings and errors

open('logfile.log', 'w').close()  # Clear previous log file

# no need to use load_dotenv() here
# docker will pass the environment variables directly to the container

def delete_client(email):
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor()


    # check to see if client exists
    # if we do not check if the client exists, we won't get an error on deletion
    # because technically nothing went wrong, the client just didn't exist
    try:
        cursor.execute("SELECT id FROM client WHERE email = %s", (email,))
        user = cursor.fetchone()
    
    # so now i only delete the client if it exists -> helps with debugging if something goes wrong as well
        if user:
            cursor.execute("DELETE FROM client WHERE email = %s", (email,))
            conn.commit()
            logger.info(f"Deleted client: {email}")
        else:
            logger.warning(f"Client with email {email} not found - nothing to delete")
    except Exception as e:
        logger.error(f"Deletion failed: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def on_message(channel, method, properties, body):
    # channel: the channel that received the message
    # method: information about the message (message metadata)
    # properties: message properties (headrs, priority, ....)
    # body: the message itself)
    try:
        xml_data = body.decode() # converting message into string
        email = ET.fromstring(xml_data).find('Email').text
        delete_client(email)
        channel.basic_ack(delivery_tag=method.delivery_tag) # 'tells' RabbitMQ that the message was processed successfully
        # the message is then removed from the queue

    except Exception as e:
        logger.error(f"Message processing failed: {e}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        # 'tells' RabbitMQ that the message processing failed
        # requeue=False -> the message is not requeued, but discarded or sent to a dead-letter queue

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
        for queue in ['kassa_user_delete', 'crm_user_delete', 'frontend_user_delete']:
            channel.queue_declare(queue=queue, durable=True)
            # queue=queue -> this is the queue we are listening to
            # durable=True -> the queue will survive a RabbitMQ server restart
            # the whole function ensures that the queues exists and are ready to receive messages
            channel.basic_consume(queue=queue, on_message_callback=on_message)
            # whenever a new message is received, the on_message_callback function is called
        
        logger.info("Listening for deletion messages...")
        channel.start_consuming()

        # you can interrupt the consumer with CTRL+C
        # this just makes sure that the connection is closed properly
    except KeyboardInterrupt:
        logger.info("Stopping consumer ...")
        channel.stop_consuming()
        connection.close()
        logger.info("Consumer stopped.")

if __name__ == "__main__":
    start_consumer()