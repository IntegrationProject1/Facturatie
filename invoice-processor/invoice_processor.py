import pika
import os
import xml.etree.ElementTree as ET
from datetime import datetime
import mysql.connector
import time
import logging
import hashlib

# For logging and debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection
def get_db_connection():
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"]
    )

# Parse XML message
def parse_order_xml(order_xml):
    try:
        root = ET.fromstring(order_xml)

        # Extract order data
        order_data = {
            'date': root.find('Date').text,
            'uuid': root.find('UUID').text,
            'products': [
                {
                    'product_nr': float(product.find('ProductNR').text),
                    'quantity': float(product.find('Quantity').text),
                    'unit_price': float(product.find('UnitPrice').text)
                }
                for product in root.find('Products').findall('Product')
            ]
        }
        return order_data
    except Exception as e:
        logger.error(f"Failed to parse order XML: {e}")
        raise

# Retrieve client by UUID
def get_client_by_uuid(uuid):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT id, first_name, last_name, email 
            FROM client 
            WHERE timestamp = %s
        """, (uuid,))
        client = cursor.fetchone()
        if not client:
            logger.error(f"No client found with UUID: {uuid}")
        return client
    except mysql.connector.Error as err:
        logger.error(f"Database error while retrieving client: {err}")
        return None
    finally:
        cursor.close()
        conn.close()

# Create invoice in FossBilling
def create_invoice(order_data, client_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Calculate totals
        subtotal = sum(
            product['quantity'] * product['unit_price'] 
            for product in order_data['products']
        )
        tax = subtotal * 0.21  # 21% VAT
        total = subtotal + tax

        # Generate unique hash
        hash_input = f"{client_id}-{datetime.utcnow().isoformat()}"
        invoice_hash = hashlib.sha256(hash_input.encode()).hexdigest()

        # Insert invoice into database
        cursor.execute("""
            INSERT INTO invoice (
                client_id, serie, nr, hash, currency, subtotal, tax, total, 
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        """, (
            client_id, 'FOSS', 1, invoice_hash, 'EUR', subtotal, tax, total
        ))
        invoice_id = cursor.lastrowid

        # Insert products into invoice_item table
        for product in order_data['products']:
            cursor.execute("""
                INSERT INTO invoice_item (
                    invoice_id, type, title, quantity, unit_price, price, taxed, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                invoice_id, 'product', f"Product {product['product_nr']}",
                product['quantity'], product['unit_price'],
                product['quantity'] * product['unit_price'], 1
            ))

        conn.commit()
        logger.info(f"Invoice created with ID: {invoice_id}")
        return invoice_hash
    except mysql.connector.Error as err:
        logger.error(f"Failed to create invoice: {err}")
        conn.rollback()
        return None
    finally:
        cursor.close()
        conn.close()

# Publish email message to RabbitMQ
def publish_email_message(invoice_hash, client, order_data):
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

        # Declare exchange
        channel.exchange_declare(exchange="invoice", exchange_type="topic", durable=True)

        # Create email XML
        email_xml = ET.Element("EmailMessage")
        ET.SubElement(email_xml, "To").text = client['email']
        ET.SubElement(email_xml, "Subject").text = "Your Invoice"
        ET.SubElement(email_xml, "Body").text = (
            f"Dear {client['first_name']} {client['last_name']},\n\n"
            f"Thank you for your order. You can download your invoice here:\n"
            f"http://integrationproject-2425s2-001.westeurope.cloudapp.azure.com:30081/invoice/pdf/{invoice_hash}\n\n"
            f"Best regards,\nE-XPO Team"
        )

        # Publish message
        channel.basic_publish(
            exchange="invoice",
            routing_key="invoice.email",
            body=ET.tostring(email_xml, encoding="unicode"),
            properties=pika.BasicProperties(delivery_mode=2)  # Persistent message
        )
        logger.info(f"Email message published for invoice hash: {invoice_hash}")
        connection.close()
    except Exception as e:
        logger.error(f"Failed to publish email message: {e}")

# Process order
def process_order(order_xml):
    try:
        # Parse order XML
        order_data = parse_order_xml(order_xml)

        # Retrieve client
        client = get_client_by_uuid(order_data['uuid'])
        if not client:
            raise ValueError(f"Client not found for UUID: {order_data['uuid']}")

        # Create invoice
        invoice_hash = create_invoice(order_data, client['id'])
        if not invoice_hash:
            raise ValueError("Failed to create invoice")

        # Publish email message
        publish_email_message(invoice_hash, client, order_data)
    except Exception as e:
        logger.error(f"Failed to process order: {e}")

# Main loop
if __name__ == "__main__":
    logger.info("Starting invoice processor")
    while True:
        try:
            # Simulate receiving an order XML (replace with RabbitMQ consumer logic)
            order_xml = """
            <Order>
                <Date>2025-05-13T12:00:00Z</Date>
                <UUID>2025-05-13T12:00:00Z</UUID>
                <Products>
                    <Product>
                        <ProductNR>1</ProductNR>
                        <Quantity>2</Quantity>
                        <UnitPrice>50.00</UnitPrice>
                    </Product>
                    <Product>
                        <ProductNR>2</ProductNR>
                        <Quantity>1</Quantity>
                        <UnitPrice>100.00</UnitPrice>
                    </Product>
                </Products>
            </Order>
            """
            process_order(order_xml)
            time.sleep(5)
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(60)