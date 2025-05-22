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

    logger.info("get_invoices() was called")

    try:
        cursor.execute("""
            SELECT 
                c.email, i.hash, i.approved, i.processed, i.created_at, i.id
            FROM invoice i
            JOIN client c ON i.client_id = c.id
            WHERE i.processed = 0 AND i.approved = 1
            ORDER BY i.created_at ASC
        """)
        invoices = cursor.fetchall()

        if not invoices:
            logger.info("No unprocessed invoices found.")
            return []
        
        for email, hash_, approved, processed, created_at, invoice_id in invoices:
            logger.info(f"Will process invoice: id={invoice_id}, hash={hash_}, approved={approved}, processed={processed}, created_at={created_at}, email={email}")
        
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
            SELECT approved FROM invoice WHERE hash = %s
        """, (invoice_hash,))
        result = cursor.fetchone()

        if result and result[0] == 1:
            cursor.execute("""
                UPDATE invoice
                SET processed = 1
                WHERE hash = %s
            """, (invoice_hash,))
            conn.commit()
            logger.info(f"Marked invoice {invoice_hash} as processed.")
    except mysql.connector.Error as err:
        logger.error(f"Error marking invoice as processed: {err}")
    finally:
        cursor.close()
        conn.close()

def create_xml_message(client_email, invoice_hash):
    host = os.environ["INVOICE_HOST"]
    port = os.environ["INVOICE_PORT"]

    invoice_pdf_url = f"http://{host}:{port}/invoice/pdf/{invoice_hash}"

    xml = ET.Element("emailMessage", attrib={"service": "facturatie"})
    # This was added upon request of the kassa team
    # according to them this was needed so that the email template is the right one for the invoices

    ET.SubElement(xml, "to").text = client_email
    ET.SubElement(xml, "from").text = "no.reply.expomail@gmail.com"
    ET.SubElement(xml, "subject").text = f"Invoice E-XPO"
    ET.SubElement(xml, "title").text = "Invoice for Your Recent Purchase"
    ET.SubElement(xml, "opener").text = "Dear Customer,"
    ET.SubElement(xml, "body").text = f"Thank you for your business! Please review the details carefully."
    ET.SubElement(xml, "footer").text = "If you have any questions, feel free to contact us at support@E-XPO.com."
    ET.SubElement(xml, "attachmenturl").text = invoice_pdf_url

    logger.info(f"Creating XML with sender: no.reply.expomail@gmail.com")
    logger.info(f"Full XML: {ET.tostring(xml, encoding='unicode')}")

    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(xml, encoding='unicode')

def send_to_rabbitmq(xml):
    queue_name = "mail_queue"
    routing_key = "mail"
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

        # Declare the email exchange
        channel.exchange_declare(
            exchange="email",
            exchange_type="topic",
            durable=True
        )

        # Declare the mail queue
        channel.queue_declare(queue=queue_name, durable=True)
        
        # Bind the queue to the exchange with the routing key
        channel.queue_bind(
            exchange="email",
            queue=queue_name,
            routing_key=routing_key
        )
        
        # Publish the message
        channel.basic_publish(
            exchange="email",
            routing_key=routing_key,
            body=xml,
            properties=pika.BasicProperties(
                delivery_mode=2  # make message persistent
            )
        )
        logger.info(f"Sent XML message to {queue_name}")

        connection.close()
        return True
    except Exception as e:
        logger.error(f"RabbitMQ Error: {e}")
        return False
    
if __name__ == "__main__":
    logger.info("Starting invoice mailing provider")
    
    while True:
        try:
            new_users = get_invoices()
            
            for user in new_users:
                xml = create_xml_message(user[0], user[1])
                # user[0] is the email, user[1] is the hash
                if send_to_rabbitmq(xml):
                    mark_as_processed(user[1])
                    # not using the uuid because a user could have multiple invoices
                    # and we don't want to send the same invoice multiple times

                    logger.info(f"Processed invoice with hash {user[1]} for client {user[0]}")
                else:
                    logger.error(f"Failed to process user {user['id']}")
            
            time.sleep(5)
        except Exception as e:
            logger.error(f"Processing error: {e}")
            time.sleep(60)
