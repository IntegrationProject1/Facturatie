import requests

def create_invoice(order_data):
    url = "http://integrationproject-2425s2-001.westeurope.cloudapp.azure.com/fossbilling/api/invoices"
    response = requests.post(url, json=order_data)
    if response.status_code == 201:
        return response.json()
    else:
        raise Exception(f"FosBilling error: {response.status_code} - {response.text}")
