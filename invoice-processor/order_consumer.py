import pika
import os
from order_processor import validate_and_parse_xml, extract_order_data
from invoice_creator import create_invoice
from rabbitmq_publisher import send_to_mailing_queue

def process_order(xml_data, xsd_path):
    try:
        # Validate and parse the XML
        order_element = validate_and_parse_xml(xml_data, xsd_path)
        order_data = extract_order_data(order_element)
        
        # Create the invoice in FossBilling
        invoice = create_invoice(order_data)
        
        # Send the invoice to the mailing queue
        send_to_mailing_queue(invoice)
        print(f"Invoice {invoice['id']} processed and sent to mailing queue.")
    except Exception as e:
        print(f"Error processing order: {e}")

def callback(ch, method, properties, body):
    xsd_path = "C:/Users/ebenh/Downloads/Facturatie/invoice-processor/order.xsd"
    process_order(body.decode(), xsd_path)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='order_queue')
    channel.basic_consume(queue='order_queue', on_message_callback=callback, auto_ack=True)
    print("Waiting for messages...")
    channel.start_consuming()

if __name__ == "__main__":
    main()