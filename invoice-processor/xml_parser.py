import xml.etree.ElementTree as ET

def parse_order(xml_string):
    root = ET.fromstring(xml_string)

    date = root.find("Date").text
    uuid = root.find("UUID").text

    products = []
    for product in root.find("Products").findall("Product"):
        product_nr = product.find("ProductNR").text
        quantity = product.find("Quantity").text
        unit_price = product.find("UnitPrice").text

        products.append({
            "product_nr": product_nr,
            "quantity": quantity,
            "unit_price": unit_price
        })

    return {
        "date": date,
        "uuid": uuid,
        "products": products
    }
