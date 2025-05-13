import pika
import os
import xml.etree.ElementTree as ET
import logging
import mysql.connector
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
    )

def get_invoices():
    conn= get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT 
                c.email, i.hash
            FROM invoice i
            JOIN client c ON i.client_id = c.id
            WHERE i.processed = 0
            ORDER BY i.created_at ASC
        """)
        invoices = cursor.fetchall()

        if not invoices:
            logger.info("No unprocessed invoices found.")
            return []
        
        return invoices
    
    except mysql.connector.Error as err:
        logger.error(f"Error fetching invoices: {err}")
        return []
    
    finally:
        cursor.close()
        conn.close()

def mark_as_processed(invoice_hash):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            UPDATE invoice
            SET processed = 1
            WHERE hash = %s
        """, (invoice_hash,))
        conn.commit()
    except mysql.connector.Error as err:
        logger.error(f"Error marking invoice as processed: {err}")
    finally:
        cursor.close()
        conn.close()

def create_xml_message(client_email, invoice_hash):
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")

    invoice_pdf_url = f"http://{host}:{port}/invoice/pdf/{invoice_hash}"

    xml = ET.Element("emailMessage")


    # Create the email body elements
    ET.SubElement(xml, "to").text = client_email
    ET.SubElement(xml, "from").text = "no-reply@E-XPO.com"
    ET.SubElement(xml, "subject").text = f"Invoice E-XPO"
    ET.SubElement(xml, "title").text = "Invoice for Your Recent Purchase"
    ET.SubElement(xml, "opener").text = "Dear Customer,"
    ET.SubElement(xml, "body").text = f"Thank you for your business! Here is your invoice ({invoice_pdf_url}). Please review the details carefully."
    ET.SubElement(xml, "footer").text = "If you have any questions, feel free to contact us at support@E-XPO.com."

    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(xml, encoding='unicode')

def send_to_rabbitmq(xml):
    queues = ["mail_queue"]
    try:
        params = pika.ConnectionParameters(
            host=os.environ["RABBITMQ_HOST"],
            port=int(os.environ["RABBITMQ_PORT"]),
            virtual_host="/",
            credentials=pika.PlainCredentials(
                os.environ["RABBITMQ_USER"],
                os.environ["RABBITMQ_PASSWORD"]
            ),
            heartbeat=600,
            blocked_connection_timeout=300
        )
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.exchange_declare(
            exchange="user",
            exchange_type="topic",  # Changed from 'direct' to 'topic'
            durable=True
        )

        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)
            # Ensure the routing pattern matches topic exchange expectations
            channel.queue_bind(
                exchange="invoice",
                queue=queue,
                routing_key=f"invoice.{queue}"
            )
            channel.basic_publish(
                exchange="invoice",
                routing_key=f"invoice.{queue}",
                body=xml,
                properties=pika.BasicProperties(
                    delivery_mode=2  # Make messages persistent
                )
            )
            logger.info(f"Sent XML message to {queue}")

        connection.close()
        return True
    except Exception as e:
        logger.error(f"RabbitMQ Error: {e}")
        return False
    
if __name__ == "__main__":
    logger.info("Starting user creation provider")
    
    while True:
        try:
            new_users = get_invoices()
            
            for user in new_users:
                xml = create_xml_message(user)
                if send_to_rabbitmq(xml):
                    mark_as_processed(user['id'])
                    logger.info(f"Processed user {user['id']} with timestamp ID {user['timestamp']}")
                else:
                    logger.error(f"Failed to process user {user['id']}")
            
            time.sleep(5)
        except Exception as e:
            logger.error(f"Processing error: {e}")
            time.sleep(60)
