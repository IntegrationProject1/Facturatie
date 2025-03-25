import mysql.connector

def send_to_database(xml_message):
    try:
        conn = mysql.connector.connect(
            host="db",
            user="admin",
            password="Admin123!",
            database="facturatie"
        )
        
        cursor = conn.cursor()
        cursor.execute("INSERT INTO user_queue (xml_message) VALUES (%s)", (xml_message,))
        
        conn.commit()
        print("Gebruiker opgeslagen in databasequeue")
    except mysql.connector.Error as err:
        print(f"Fout bij databaseverbinding: {err}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

