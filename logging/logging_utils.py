import pika
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_connection():
    try:
        params = pika.ConnectionParameters(
            host=os.environ["RABBITMQ_HOST"],
            port=int(os.environ["RABBITMQ_PORT"]),
            credentials=pika.PlainCredentials(
                os.environ["RABBITMQ_USER"],
                os.environ["RABBITMQ_PASSWORD"]
            ),
            heartbeat=600,
            blocked_connection_timeout=300
        )
        return pika.BlockingConnection(params)
    except Exception as e:
        logger.error(f"RabbitMQ connectie fout: {e}")
        return None

def log_to_xml(data):
    xml = f"""
    <Log>
        <ServiceName>{data['ServiceName']}</ServiceName>
        <Status>{data['Status']}</Status>
        <Code>{data['Code']}</Code>
        <Message>{data['Message']}</Message>
        <Level>{data['Level']}</Level>
    </Log>
    """
    return xml.strip()

def send_log(service_name, status, code, message, level):
    data = {
        "ServiceName": service_name,
        "Status": status,
        "Code": code,
        "Message": message,
        "Level": level
    }
    message_xml = log_to_xml(data)

    connection = get_connection()
    if connection:
        try:
            channel = connection.channel()
            channel.exchange_declare(exchange='log_monitoring', exchange_type='fanout', durable=True)
            channel.basic_publish(
                exchange='log_monitoring',
                routing_key='',
                body=message_xml,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            logger.info("Log verzonden: %s", level)
            connection.close()
        except Exception as e:
            logger.error(f"Fout bij verzenden log: {e}")
    else:
        logger.error("Kon geen verbinding maken met RabbitMQ")
