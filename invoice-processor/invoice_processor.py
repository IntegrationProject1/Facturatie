import os
import json
import requests
from dotenv import load_dotenv
from xml_parser import parse_order
from publisher import publish_invoice

# .env inladen
load_dotenv()

# FosBilling API-configuratie
FOSBILLING_API_URL = os.getenv("FOSBILLING_API", "http://localhost/api/invoices")

def process_invoice(xml_string):
    try:
        # Parse de XML naar een Python dict
        order_data = parse_order(xml_string)
        print("📦 Parsed order:", order_data)

        # Maak een invoice payload
        invoice_payload = {
            "customer": {
                "name": "Klant Naam",
                "email": "klant@example.com"
            },
            "items": []
        }

        total = 0
        for product in order_data['products']:
            line_total = float(product['quantity']) * float(product['unit_price'])
            total += line_total
            invoice_payload["items"].append({
                "description": f"Product {product['product_nr']}",
                "quantity": float(product['quantity']),
                "unit_price": float(product['unit_price']),
                "total": line_total
            })

        invoice_payload["total"] = total

        # Verstuur naar FosBilling
        response = requests.post(FOSBILLING_API_URL, json=invoice_payload)

        if response.status_code == 201:
            invoice_id = response.json().get("id", "UNKNOWN")
            print(f"🧾 Invoice created successfully: {invoice_id}")

            # Verstuur naar mailing queue
            publish_invoice(invoice_id)
        else:
            print("❌ Failed to create invoice:", response.status_code, response.text)

    except Exception as e:
        print("❌ Error processing invoice:", e)
