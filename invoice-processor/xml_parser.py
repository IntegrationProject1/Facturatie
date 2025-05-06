import xml.etree.ElementTree as ET

def parse_order(xml_string):
    root = ET.fromstring(xml_string)
    products = []
    for product in root.find('Products').findall('Product'):
        products.append({
            'product_nr': float(product.find('ProductNR').text),
            'quantity': float(product.find('Quantity').text),
            'unit_price': float(product.find('UnitPrice').text),
        })
    return {
        'date': root.find('Date').text,
        'uuid': root.find('UUID').text,
        'products': products
    }
