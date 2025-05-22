import hashlib
import json
import pika
import os
import logging
import xml.etree.ElementTree as ET
import mysql.connector

# Logging instellen
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# first checking if the invoice already exists
# no exception handling here because this is just to return something.
# if the invoice already exists, it's checked later on in the create_invoice function
# def invoice_exists(data):
#     conn = mysql.connector.connect(
#         host=os.getenv("DB_HOST"),
#         user=os.getenv("DB_USER"),
#         password=os.getenv("DB_PASSWORD"),
#         database=os.getenv("DB_NAME")
#     )
#     cursor = conn.cursor()
#     try:
#         created_at = data['Date']
#         cursor.execute("SELECT id FROM invoice WHERE created_at = %s", (created_at,))
#         return cursor.fetchone() is not None
#     finally:
#         cursor.close()
#         conn.close()

# since the XSD provides us with the UUID of the user, we can use that to get the client_id
# the client_id is necessary to insert the invoice into the database and to later send the email
def get_client_by_uuid(uuid):
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id, email, first_name, last_name, phone_cc, phone, company, company_vat, company_number, city, state, postcode, country, currency, address_1 FROM client WHERE timestamp = %s", (uuid,))
        result = cursor.fetchone()
        if result:
            logger.info(f"Client found for UUID: {uuid}, ID: {result[0]}, Email: {result[1]}")
            return {'id': result[0], 'email': result[1], 'first_name': result[2], 'last_name': result[3], 'phone_cc': result[4], 'phone': result[5],
                    'company': result[6], 'company_vat': result[7], 'company_number': result[8], 'city': result[9], 'state': result[10],
                    'postcode': result[11], 'country': result[12], 'currency': result[13], 'address_1': result[14]}
        else:
            logger.warning(f"No client found for UUID: {uuid}")
            return None
    finally:
        cursor.close()
        conn.close()

# Parse the XML data
def parse_invoice_xml(xml_data):
    try:
        root = ET.fromstring(xml_data)

        # Log incoming date before processing
        raw_date = root.findtext('Date')
        logger.info(f"Incoming raw date: {raw_date}")

        # Process the date
        date_str = raw_date
        if date_str and date_str.endswith('Z'):
            date_str = date_str[:-1]  # Remove the last character ('Z')
            logger.info(f"Removed 'Z' from date. Parsed date: {date_str}")
        else:
            logger.info(f"No 'Z' found in date. Using original date: {date_str}")

        invoice_data = {
            'uuid': root.findtext('UUID'),
            'date': date_str,
            'products': []
        }

        products = root.find('Products')
        if products is not None:
            for product in products.findall('Product'):
                invoice_data['products'].append({
                    'product_id': product.findtext('ProductNR'),
                    'quantity': product.findtext('Quantity'),
                    'price': product.findtext('UnitPrice'),
                    'name': product.findtext('ProductNaam')
                })

        logger.info(f"Successfully parsed invoice with UUID: {invoice_data['uuid']}")
        logger.info(f"Final parsed date in invoice data: {invoice_data['date']}")
        return invoice_data
    except Exception as e:
        logger.error(f"XML parsing failed: {e}")
        raise

# since we're using a hash to 1. check if the invoice already exists
# and 2. to later send the link to the invoice to the mailing team, we need to generate a hash
def generate_invoice_hash(data):
    
    hash_input = {
        'uuid': data['uuid'],
        'products': data['products'],
        'date': data['date'],
    }
    hash_str = json.dumps(hash_input, sort_keys=True)

    logger.info(f"Invoice hash generated !")
    
    return hashlib.sha256(hash_str.encode('utf-8')).hexdigest()

def create_invoice(data):
    client_info = get_client_by_uuid(data['uuid'])
    if not client_info:
        raise ValueError("Client not found for UUID: " + data['uuid'])

    client_id = client_info['id']
    client_email = client_info['email']
    client_first_name = client_info['first_name']
    client_last_name = client_info['last_name']
    client_phone_cc = client_info['phone_cc']
    client_phone = client_info['phone']
    client_company = client_info['company']
    client_company_vat = client_info['company_vat']
    client_company_number = client_info['company_number']
    client_city = client_info['city']
    client_state = client_info['state']
    client_postcode = client_info['postcode']
    client_country = client_info['country']
    client_currency = client_info['currency']
    client_address_1 = client_info['address_1']



    invoice_hash = generate_invoice_hash(data)

    logger.info(f"Invoice hash generated: {invoice_hash}")

    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        cursor = conn.cursor()

        # Insert invoice
        cursor.execute("""
            INSERT INTO invoice (
                client_id, hash, status, approved, created_at, buyer_email, buyer_first_name, 
                buyer_last_name, buyer_phone_cc, buyer_phone, buyer_company, buyer_company_vat, 
                buyer_company_number, buyer_city, buyer_state, buyer_zip, buyer_country, currency, buyer_address,
                seller_company, seller_company_vat, seller_company_number, seller_address, seller_phone, seller_email
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s)

        """, (
            client_id,
            invoice_hash,
            'unpaid', # status is almost always unpaid. If this is not the case, the XSD must be changed
            1,
            data['date'],  # Use date from XML
            client_email,
            client_first_name,
            client_last_name,
            client_phone_cc,
            client_phone,
            client_company,
            client_company_vat,
            client_company_number,
            client_city,
            client_state,
            client_postcode,
            client_country,
            client_currency,
            client_address_1,
            'E-XPO',
            'BE4598792446749',
            'E-XPO',
            'Nijverheidskaai 170 Anderlecht 1070 België',
            '+32 465 49 44 79',
            'no.reply.expomail@gmail.com'

        ))

        invoice_id = cursor.lastrowid

        # Insert invoice items
        for product in data['products']:
            cursor.execute("""
                INSERT INTO invoice_item (
                    invoice_id, quantity, price, title, created_at
                ) VALUES (%s, %s, %s, %s, NOW())
            """, (
                invoice_id,
                product['quantity'],
                product['price'],
                product['name']
            ))

        conn.commit()
        logger.info(f"Invoice inserted with hash {invoice_hash}")
        return True
    except Exception as e:
        logger.error(f"Error creating invoice: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# Callback functie
def on_message(channel, method, properties, body):
    try:
        logger.info(f"Message received via {method.routing_key}")
        invoice_data = parse_invoice_xml(body.decode())

        create_invoice(invoice_data)
        channel.basic_ack(method.delivery_tag)
    except Exception as e:
        logger.error(f"Processing error: {e}")
        channel.basic_nack(method.delivery_tag, requeue=False)

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
        queues = ['order.created']
        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)
            channel.basic_consume(
                queue=queue,
                on_message_callback=on_message,
                auto_ack=False
            )

        logger.info("Wachten op facturatieberichten...")
        channel.start_consuming()

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
        channel.stop_consuming()
        connection.close()
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        raise

if __name__ == "__main__":
    start_consumer()