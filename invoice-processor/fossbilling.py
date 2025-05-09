# fossbilling.py
import requests
import json
import os

FOSS_API_URL = "https://your-fossbilling-api-endpoint.com"
FOSS_API_KEY = os.environ["FOSS_API_KEY"]

def generate_invoice(order_data):
    """Generates an invoice using FossBilling API."""
    invoice_data = {
        'customer_id': get_customer_id(order_data['UUID']),
        'date': order_data['date'],
        'products': order_data['Products'],
        'total_amount': calculate_total_amount(order_data['Products'])
    }
    
    # Call FossBilling API to create the invoice
    response = requests.post(f"{FOSS_API_URL}/invoices", json=invoice_data, headers={"Authorization": f"Bearer {FOSS_API_KEY}"})
    
    if response.status_code == 200:
        invoice = response.json()
        return invoice
    else:
        raise Exception(f"Failed to create invoice: {response.text}")

def get_customer_id(UUID):
    """Get the customer ID based on the UUID."""
    # Assuming a database call or an API call to fetch the customer ID
    return 123  # Placeholder for actual customer ID fetching logic

def calculate_total_amount(products):
    """Calculate the total amount based on products' quantity and unit price."""
    total = 0
    for product in products:
        total += float(product['Quantity']) * float(product['UnitPrice'])
    return total

def send_to_mailing_queue(invoice):
    """Sends the created invoice to the mailing queue."""
    # Create the mailing message (example format)
    email_message = {
        "to": "customer@example.com",
        "subject": "Your Invoice from Company",
        "htmlcontent": f"""
        <html>
            <body>
                <h1>Your Invoice #{invoice['id']}</h1>
                <p>Thank you for your order. Your invoice details are as follows:</p>
                <ul>
                    {''.join([f"<li>{product['ProductNR']} - {product['Quantity']} x {product['UnitPrice']}</li>" for product in invoice['products']])}
                </ul>
                <p>Total Amount: {invoice['total_amount']}</p>
                <p>If you have any questions, feel free to contact us.</p>
            </body>
        </html>
        """
    }
    
    # Send the email message to the mailing queue
    # (RabbitMQ code to publish this message will go here)
    pass
