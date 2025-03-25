import mysql.connector
import xml.etree.ElementTree as ET

def update_user_from_xml(xml_message):
    # Parse the XML message
    root = ET.fromstring(xml_message)
    
    # Extract user information from the XML
    user_id = root.find('UserID').text
    first_name = root.find('FirstName').text
    last_name = root.find('LastName').text
    phone_number = root.find('PhoneNumber').text
    email_address = root.find('EmailAddress').text
    business_name = root.find('Business/BusinessName').text
    business_email = root.find('Business/BusinessEmail').text
    real_address = root.find('Business/RealAddress').text
    btw_number = root.find('Business/BTWNumber').text
    facturation_address = root.find('Business/FacturationAddress').text

    # Connect to the MySQL database
    conn = mysql.connector.connect(
        host="db",
        user="admin",
        password="Admin123!",
        database="facturatie"
    )
    
    cursor = conn.cursor()
    
    # Execute the update statement
    cursor.execute("""
        UPDATE users 
        SET first_name = %s, last_name = %s, phone_number = %s, email_address = %s,
            business_name = %s, business_email = %s, real_address = %s, 
            btw_number = %s, facturation_address = %s
        WHERE user_id = %s
    """, (first_name, last_name, phone_number, email_address,
          business_name, business_email, real_address, 
          btw_number, facturation_address, user_id))
    
    # Commit the changes
    conn.commit()
    
    if cursor.rowcount > 0:
        print(f"User with ID {user_id} updated successfully.")
    else:
        print(f"No user found with ID {user_id}. No updates made.")
    
    # Close the cursor and connection
    cursor.close()
    conn.close()

    # Example XML message
xml_message = """<?xml version="1.0" encoding="UTF-8"?>
<UserMessage>
    <ActionType>UpdateUser</ActionType>
    <UserID>67890</UserID>
    <TimeOfAction>2025-03-25T15:00:00</TimeOfAction>
    <FirstName>Amine</FirstName>
    <LastName>Zerouali</LastName>
    <PhoneNumber>+32456789012</PhoneNumber>
    <EmailAddress>amine@example.com</EmailAddress>
    <Business>
        <BusinessName>AZ Web Solutions</BusinessName>
        <BusinessEmail>contact@azwebsolutions.com</BusinessEmail>
        <RealAddress>Brussels, Belgium</RealAddress>
        <BTWNumber>BE123456789</BTWNumber>
        <FacturationAddress>Brussels, Belgium</FacturationAddress>
    </Business>
</UserMessage>"""
# Example usage
update_user_from_xml(xml_message)