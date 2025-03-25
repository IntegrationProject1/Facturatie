import mysql.connector
from dotenv import load_dotenv
import os

def send_to_database(xml_message):
    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
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

