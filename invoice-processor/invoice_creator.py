import mysql.connector
import os
from datetime import datetime

def create_invoice(order_data):
    total_amount = sum(
        product["quantity"] * product["unit_price"]
        for product in order_data["products"]
    )
    
    # Prepare invoice data
    invoice = {
        "client_id": 1,  # Replace with the actual client ID logic if needed
        "date": datetime.strptime(order_data["date"], "%Y-%m-%dT%H:%M:%S"),
        "uuid": order_data["uuid"],
        "total_amount": total_amount,
        "currency": "USD",  # Replace with the actual currency if needed
        "status": "unpaid",  # Default status
    }
    
    # Connect to FossBilling database
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    cursor = conn.cursor()
    try:
        # Insert invoice into FossBilling database
        insert_query = """
        INSERT INTO invoices (client_id, date, uuid, total, currency, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            invoice["client_id"],
            invoice["date"],
            invoice["uuid"],
            invoice["total_amount"],
            invoice["currency"],
            invoice["status"]
        ))
        conn.commit()
        
        # Get the inserted invoice ID
        invoice_id = cursor.lastrowid
        invoice["id"] = invoice_id
        
        # Optionally, insert invoice items (products)
        for product in order_data["products"]:
            insert_item_query = """
            INSERT INTO invoice_items (invoice_id, description, quantity, unit_price, total)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_item_query, (
                invoice_id,
                f"Product {product['product_nr']}",
                product["quantity"],
                product["unit_price"],
                product["quantity"] * product["unit_price"]
            ))
        conn.commit()
    finally:
        cursor.close()
        conn.close()
    
    return invoice