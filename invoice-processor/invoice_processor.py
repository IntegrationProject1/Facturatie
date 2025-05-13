# invoice-processor/invoice_processor.py
import pika
import os
import logging
import xml.etree.ElementTree as ET
import mysql.connector
from datetime import datetime

# Logging configureren
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InvoiceProcessor:
    def __init__(self):
        self.rabbitmq_host = os.getenv("RABBITMQ_HOST")
        self.rabbitmq_port = int(os.getenv("RABBITMQ_PORT"))
        self.rabbitmq_user = os.getenv("RABBITMQ_USER")
        self.rabbitmq_pass = os.getenv("RABBITMQ_PASSWORD")
        
        self.db_config = {
            'host': os.getenv("DB_HOST"),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD"),
            'database': os.getenv("DB_NAME")
        }

    def get_client_by_uuid(self, uuid_timestamp):
        """Haal clientgegevens op basis van UUID (timestamp)"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor(dictionary=True)
            
            query = """
                SELECT id, first_name, last_name, email 
                FROM client 
                WHERE timestamp = %s
            """
            cursor.execute(query, (uuid_timestamp,))
            result = cursor.fetchone()
            
            if not result:
                logger.error(f"Geen client gevonden met timestamp: {uuid_timestamp}")
                return None
                
            return result
        except Exception as e:
            logger.error(f"Databasefout bij ophalen client: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def create_fossbilling_invoice(self, order_data, client_id):
        """Maak een factuur aan in FossBilling"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Bereken totaal
            total = sum(
                float(product['Quantity']) * float(product['UnitPrice']) 
                for product in order_data['Products']
            )
            
            # Factuur aanmaken
            invoice_query = """
                INSERT INTO invoice (
                    client_id, status, paid_at, created_at, updated_at, 
                    taxname, taxrate, number, serie, discount, 
                    subtotal, total, currency, currency_rate, 
                    due_at, seller_notes, terms, notes
                ) VALUES (%s, %s, NULL, NOW(), NOW(), 
                          %s, %s, %s, %s, %s, 
                          %s, %s, %s, %s, 
                          DATE_ADD(NOW(), INTERVAL 30 DAY), %s, %s, %s)
            """
            invoice_values = (
                client_id, 'unpaid',
                'BTW', 21.00, self.generate_invoice_number(), 'INV', 
                0.00, total, total, 'EUR', 1.00,
                '', '', 'Factuur gegenereerd vanuit order'
            )
            cursor.execute(invoice_query, invoice_values)
            invoice_id = cursor.lastrowid
            
            # Producten toevoegen
            for product in order_data['Products']:
                item_query = """
                    INSERT INTO invoice_item (
                        invoice_id, type, title, description, 
                        quantity, unit_price, price, taxed, 
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """
                item_values = (
                    invoice_id, 'product', f"Product {product['ProductNR']}", 
                    f"Productnummer: {product['ProductNR']}", 
                    product['Quantity'], product['UnitPrice'], 
                    float(product['Quantity']) * float(product['UnitPrice']), 
                    1
                )
                cursor.execute(item_query, item_values)
            
            conn.commit()
            logger.info(f"Factuur aangemaakt met ID: {invoice_id}")
            return invoice_id
        except Exception as e:
            logger.error(f"Fout bij aanmaken factuur: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def generate_invoice_number(self):
        """Genereer een uniek factuurnummer"""
        now = datetime.now()
        return f"INV-{now.year}{now.month:02d}{now.day:02d}-{now.hour:02d}{now.minute:02d}{now.second:02d}"

    def prepare_email_message(self, invoice_id, client_data, order_data):
        """Bereid het emailbericht voor"""
        email_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<emailMessage service="facturatie">
    <to>{client_data['email']}</to>
    <from>expo-facturatie@gmail.com</from>
    <subject>Uw factuur #{invoice_id}</subject>
    <title>Factuur #{invoice_id}</title>
    <opener></opener>
    <body>
        Beste {client_data['first_name']} {client_data['last_name']},
        
        Hierbij ontvangt u de factuur voor uw recente bestelling.
        
        Factuurnummer: {invoice_id}
        Datum: {order_data['Date']}
        
        Met vriendelijke groeten,
        Het Facturatie Team
    </body>
    <footer>
        © {datetime.now().year} E-XPO
    </footer>
</emailMessage>
        """
        return email_xml

    def publish_email_message(self, email_xml):
        """Publiceer het emailbericht naar de mail_queue"""
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=self.rabbitmq_host,
                port=self.rabbitmq_port,
                credentials=pika.PlainCredentials(
                    self.rabbitmq_user,
                    self.rabbitmq_pass
                )
            ))
            channel = connection.channel()
            
            channel.queue_declare(queue='mail_queue', durable=True)
            channel.basic_publish(
                exchange='',
                routing_key='mail_queue',
                body=email_xml,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                )
            )
            
            logger.info("Emailbericht gepubliceerd naar mail_queue")
            connection.close()
        except Exception as e:
            logger.error(f"Fout bij publiceren naar RabbitMQ: {e}")
            raise

    def process_order(self, order_xml):
        """Verwerk de order en genereer factuur en email"""
        try:
            # Parse XML
            root = ET.fromstring(order_xml)
            
            order_data = {
                'Date': root.find('Date').text,
                'UUID': root.find('UUID').text,
                'Products': [
                    {
                        'ProductNR': product.find('ProductNR').text,
                        'Quantity': product.find('Quantity').text,
                        'UnitPrice': product.find('UnitPrice').text
                    }
                    for product in root.find('Products').findall('Product')
                ]
            }
            
            # Haal clientgegevens op
            client_data = self.get_client_by_uuid(order_data['UUID'])
            if not client_data:
                raise ValueError(f"Client niet gevonden met UUID: {order_data['UUID']}")
            
            # Maak factuur aan
            invoice_id = self.create_fossbilling_invoice(order_data, client_data['id'])
            
            # Bereid email voor
            email_xml = self.prepare_email_message(invoice_id, client_data, order_data)
            
            # Publiceer email
            self.publish_email_message(email_xml)
            
            return True
        except Exception as e:
            logger.error(f"Fout bij verwerken order: {e}")
            raise

    def start_consuming(self):
        """Start de RabbitMQ consumer"""
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=self.rabbitmq_host,
                port=self.rabbitmq_port,
                credentials=pika.PlainCredentials(
                    self.rabbitmq_user,
                    self.rabbitmq_pass
                )
            ))
            channel = connection.channel()
            
            channel.queue_declare(queue='order.created', durable=True)
            channel.basic_consume(
                queue='order.created',
                on_message_callback=self.on_message,
                auto_ack=False
            )
            
            logger.info("Wachten op orders op queue 'order.created'...")
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Consumer gestopt")
            connection.close()
        except Exception as e:
            logger.error(f"Consumer fout: {e}")
            raise

    def on_message(self, channel, method, properties, body):
        """Callback voor binnenkomende berichten"""
        try:
            logger.info(f"Order ontvangen via {method.routing_key}")
            self.process_order(body.decode())
            channel.basic_ack(method.delivery_tag)
        except Exception as e:
            logger.error(f"Fout bij verwerken bericht: {e}")
            channel.basic_nack(method.delivery_tag, requeue=False)

if __name__ == "__main__":
    processor = InvoiceProcessor()
    processor.start_consuming()