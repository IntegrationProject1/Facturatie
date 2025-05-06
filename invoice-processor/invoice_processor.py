from xml_parser import parse_order
from fosbilling_client import create_invoice
from publisher import send_to_mailing

def process_invoice(xml_string):
    order_data = parse_order(xml_string)
    invoice = create_invoice(order_data)
    send_to_mailing(invoice)
