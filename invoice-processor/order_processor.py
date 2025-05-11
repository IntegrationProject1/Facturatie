import xmlschema
import xml.etree.ElementTree as ET

def validate_and_parse_xml(xml_data, xsd_path):
    schema = xmlschema.XMLSchema(xsd_path)
    if schema.is_valid(xml_data):
        return ET.fromstring(xml_data)
    else:
        # Print detailed validation errors
        for error in schema.iter_errors(xml_data):
            print(f"Validation error: {error}")
        raise ValueError("Invalid XML data")

def extract_order_data(order_element):
    order_data = {
        "date": order_element.find("Date").text,
        "uuid": order_element.find("UUID").text,
        "products": []
    }
    for product in order_element.find("Products").findall("Product"):
        order_data["products"].append({
            "product_nr": float(product.find("ProductNR").text),
            "quantity": float(product.find("Quantity").text),
            "unit_price": float(product.find("UnitPrice").text)
        })
    return order_data