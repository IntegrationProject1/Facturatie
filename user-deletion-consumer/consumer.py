import pika
import json
import logging
import mysql.connector
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
    )


def find_user_by_email(email):

    db_connection = get_db_connection()
    try:
        with db_connection.cursor() as cursor: # Tries to find the user using their email
            sql = "SELECT id FROM client WHERE email = %s"
            cursor.execute(sql, (email,))
            result = cursor.fetchone() # ?
            return result['id'] if result else None
    except Exception as e:
        logger.error(f"Error finding user by email {email}: {str(e)}")
        return None
    
def delete_user(email):
    db_connection = get_db_connection()

    try:
        with db_connection.cursor() as cursor:
            # Delete related records first
            # tables = [
            #     'client_balance',
            #     'client_order',
            #     'invoice',
            #     'activity',
            #     # Add other related tables as needed
            #     # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            # ]
            
            # for table in tables:
            #     try:
            #         cursor.execute(f"DELETE FROM {table} WHERE id = %s", (id,)) # hier moet er weer met de email gewerkt worden
            #     except Exception as e:
            #         logger.warning(f"Could not delete from {table}: {str(e)}")
            
            # Delete the client
            cursor.execute("DELETE FROM client WHERE email = %s", (email,)) # hier moet er weer met de email gewerkt worden
            db_connection.commit()
            return True
    except Exception as e:
        db_connection.rollback() # Rollback the transaction if an error occurs
        logger.error(f"Error deleting user {email}: {str(e)}")
        return False
    
def process_message(ch, method, properties, body):

    try:
        message = json.loads(body) 
        email = message.get('email')
        
        if not email:
            logger.error("Message missing email field")
            ch.basic_nack(delivery_tag=method.delivery_tag)
            return
        
        logger.info(f"Processing deletion request for email: {email}")
        
        client_id = find_user_by_email(email)
        if not client_id:
            logger.warning(f"User with email {email} not found")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        
        if delete_user(client_id):
            logger.info(f"Successfully deleted user {email} (ID: {client_id})")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
             logger.error(f"Failed to delete user {email}")
             ch.basic_nack(delivery_tag=method.delivery_tag)
    
    except json.JSONDecodeError:
        logger.error("Invalid JSON message received")
        ch.basic_nack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Unexpected error processing message: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag)

def rabbitmq_connection():

    credentials = pika.PlainCredentials(os.environ["RABBITMQ_USER"], os.environ["RABBITMQ_PASSWORD"])
    parameters = pika.ConnectionParameters(
        host=os.environ["RABBITMQ_HOST"],
        port=os.environ["RABBITMQ_PORT"],
        credentials=credentials,
    )
    
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    # These queues can also be placed inside the .env file for more security
    # but that's an improvement up for discussion with the team later when improving the code
    RABBITMQ_QUEUES = [
        "crm_user_delete",
        "frontend_user_delete",
        "facturatie_user_delete", # to delete later -> just for testing
        "kassa_user_delete"
    ]
    for queue in RABBITMQ_QUEUES:
        channel.queue_declare(queue=queue, durable=True)
    
    return connection, channel

def main():
    # Debug test
    print("Testing database connection...")
    try:
        conn = get_db_connection()
        conn.close()
        print("Database connection test successful!")
    except Exception as e:
        print(f"Database connection test failed: {e}")
        return

    try:
        connection, channel = rabbitmq_connection()
        
        queues = [
            "crm_user_delete",
            "frontend_user_delete", 
            "facturatie_user_delete",
            "kassa_user_delete"
        ]
        
        for queue in queues:
            channel.basic_consume(
                queue=queue,
                on_message_callback=process_message,  # Removed the extra parameter
                auto_ack=False
            )
        
        logger.info("Waiting for messages. To exit press CTRL+C")
        channel.start_consuming()
    
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
    finally:
        if 'channel' in locals():
            channel.stop_consuming()
        if 'connection' in locals():
            connection.close()

if __name__ == '__main__':
    main()