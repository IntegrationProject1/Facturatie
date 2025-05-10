from order_consumer import process_order

# Test XML data (voorbeeld)
test_xml = """
<Order>
    <Date>2023-10-01T12:00:00</Date>
    <UUID>2023-10-01T12:00:00</UUID>
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

# Test XSD path (vervang dit met het pad naar je XSD-bestand)
xsd_path = "C:/Users/ebenh/Downloads/Facturatie/invoice-processor/order.xsd"

# Test de process_order-functie
def test_process_order():
    try:
        process_order(test_xml, xsd_path)
        print("Test geslaagd: Order succesvol verwerkt.")
    except Exception as e:
        print(f"Test mislukt: {e}")

if __name__ == "__main__":
    test_process_order()

    from order_consumer import callback

# Mock RabbitMQ-bericht
class MockChannel:
    def basic_ack(self, delivery_tag):
        print("Mock: Acknowledged message.")

mock_channel = MockChannel()
mock_method = None
mock_properties = None
mock_body = test_xml.encode()  # Simuleer een RabbitMQ-bericht

# Test de callback-functie
def test_callback():
    try:
        callback(mock_channel, mock_method, mock_properties, mock_body)
        print("Test geslaagd: Callback succesvol uitgevoerd.")
    except Exception as e:
        print(f"Test mislukt: {e}")

if __name__ == "__main__":
    test_callback()

    from unittest.mock import patch

@patch("mysql.connector.connect")
def test_create_invoice(mock_connect):
    mock_connect.return_value.cursor.return_value.execute.return_value = None
    # Voeg hier je testlogica toe