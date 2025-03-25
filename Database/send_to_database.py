import mysql.connector

def send_to_database():
    conn = mysql.connector.connect(
        host="localhost",
        user="jouwuser",
        password="jouwwachtwoord",
        database="jouwdb"
    )
    
    cursor = conn.cursor()
    cursor.execute("INSERT INTO user_queue (xml_message) VALUES (%s)", (xml_message,))
    
    conn.commit()
    print("Gebruiker opgeslagen in databasequeue")
    
    cursor.close()
    conn.close()
