import pika
import os
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv  # so python can read the .env file

# loading env variables from .env file
load_dotenv()

# define logger to log messages to console 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# RabbitMQ connection setup with credentials from .env file
def get_rabbitmq_connection():
    try:
        params = pika.ConnectionParameters(  # connection parameters
            host=os.environ["RABBITMQ_HOST"], 
            port=int(os.environ["RABBITMQ_PORT"]),
            virtual_host="/",
            credentials=pika.PlainCredentials(
                os.environ["RABBITMQ_USER"],
                os.environ["RABBITMQ_PASSWORD"]
            ),
            heartbeat=600,  # heartbeat interval
            blocked_connection_timeout=300 # timeout for blocked connections
        )
        return pika.BlockingConnection(params)
    except Exception as e:
        logger.error(f"Fout bij verbinden met RabbitMQ: {e}")  # log error when connection fails
        return None


# Convert dictionary to the specified XML format 
def dict_to_xml(log): 
    """Convert dictionary to the specified XML format."""
    xml = """
    <Heartbeat>
        <ServiceName>{ServiceName}</ServiceName>
        <Status>{Status}</Status>
        <Timestamp>{Timestamp}</Timestamp>
        <HeartBeatInterval>{HeartBeatInterval}</HeartBeatInterval>
        <Metadata>
            <Version>{Version}</Version>
            <Host>{Host}</Host>
            <Environment>{Environment}</Environment>
        </Metadata>
    </Heartbeat>
    """.format(**log) 
    return xml.strip()


# Prepare heartbeat data
def create_heartbeat_message():
    # Define the heartbeat dictionary with necessary data
    heartbeat_data = {
        "ServiceName": "Facturatie", # Example service name (can be adjusted)
        "Status": "Online", # Example status (can be adjusted)
        "Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),  # Current time in UTC
        "HeartBeatInterval": "60",  # Interval in seconds (adjust as needed)
        "Version": "1.0",  # Example version
        "Host": os.environ["RABBITMQ_HOST"],  # Get the host from environment
        "Environment": "Production"  # Define environment (can be adjusted)
    }

    # Convert dictionary to XML
    return dict_to_xml(heartbeat_data)


# Heartbeat to rabbitmq exchange
def send_heartbeat():
    connection = get_rabbitmq_connection()

    if connection:
        try:
            channel = connection.channel()

            # Declare a queue (controlroom.heartbeat.ping)
            channel.queue_declare(queue="controlroom_heartbeat", durable=True)  # Ensure queue durability

            heartbeat_xml = create_heartbeat_message()  # Create heartbeat message

            # Send message to the queue 'controlroom_heartbeat' (use appropriate routing key)
            channel.basic_publish(
                exchange="heartbeat",  # The exchange we're using (can be find in confluence)
                routing_key="controlroom_heartbeat",  # Route to the correct queue
                body=heartbeat_xml,
                properties=pika.BasicProperties(
                    delivery_mode=2  # Make the message persistent
                )
            )

            logger.info("Heartbeat verzonden naar ControlRoom")  # Log message when heartbeat is sent
            connection.close()
        except Exception as e:
            logger.error(f"Fout bij verzenden van heartbeat: {e}") # Log error when sending fails
    else:
        logger.error("Kan geen verbinding maken met RabbitMQ!") # Log error when connection fails


# Main loop: each second send a heartbeat
if __name__ == "__main__":
    logger.info("Heartbeat sender gestart")
    while True:
        send_heartbeat() 
        time.sleep(1)  # Send heartbeat every second (you can adjust the interval)
