import time
from scripts.db.db_connector import get_db_connection
from scripts.xml.create_user_xml import create_user_xml
from scripts.rabbitmq.send_to_queue import send_to_queue

def poll_for_new_users():
    while True:

        connection = get_db_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM clients WHERE is_processed = 0 ORDER BY created_at DESC LIMIT 1")
                new_user = cursor.fetchone()
                if new_user:

                    xml_file_path = create_user_xml(new_user)

                    send_to_queue(xml_file_path)

                    cursor.execute("UPDATE clients SET is_processed = 1 WHERE user_id = %s", (new_user['user_id'],))
                    connection.commit()
        finally:
            connection.close()

        time.sleep(60)

poll_for_new_users()