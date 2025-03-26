import xml.etree.ElementTree as ET
from datetime import datetime

def create_user_xml(user_data):
    root = ET.Element("UserMessage")
    
    action_type = ET.SubElement(root, "ActionType")
    action_type.text = "CreateUser"

    user_id = ET.SubElement(root, "UserID")
    user_id.text = str(user_data['user_id'])
    
    time_of_action = ET.SubElement(root, "TimeOfAction")
    time_of_action.text = datetime.now().isoformat()

    first_name = ET.SubElement(root, "FirstName")
    first_name.text = user_data["first_name"]

    last_name = ET.SubElement(root, "LastName")
    last_name.text = user_data["last_name"]

    phone_number = ET.SubElement(root, "PhoneNumber")
    phone_number.text = f"+{user_data['phone_cc']}{user_data['phone']}"

    email_address = ET.SubElement(root, "EmailAddress")
    email_address.text = user_data["email"]

    business = ET.SubElement(root, "Business")

    business_name = ET.SubElement(business, "BusinessName")
    business_name.text = user_data["company"]

    business_email = ET.SubElement(business, "BusinessEmail")
    business_email.text = user_data["company_email"]

    real_address = ET.SubElement(business, "RealAddress")
    real_address.text = user_data["address_1"]

    btw_number = ET.SubElement(business, "BTWNumber")
    btw_number.text = user_data["company_vat"]

    facturation_address = ET.SubElement(business, "FacturationAddress")
    facturation_address.text = user_data["address_2"]
    
    tree = ET.ElementTree(root)
    xml_file_path = "create_user.xml"
    tree.write(xml_file_path, encoding="UTF-8", xml_declaration=True)
    return xml_file_path
