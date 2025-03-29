import pika
from defusedxml.ElementTree import fromstring as safe_xml_parse
import json
import logging
import mysql.connector.pooling
import os
import time
from dotenv import load_dotenv

# Initialize environment variables from .env file
load_dotenv()

# Set up logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.FileHandler('user_updates.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'database': os.getenv("DB_NAME")
}

# Manages database connections using connection pooling for efficiency
class DatabaseManager:
    _connection_pool = None

# Initialize connection pool with retry logic for robustness and error handling
    @classmethod
    def initialize_pool(cls):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                cls._connection_pool = mysql.connector.pooling.MySQLConnectionPool(
                    pool_name="foss_pool",
                    pool_size=5,
                    **DB_CONFIG,
                    autocommit=False,
                    connect_timeout=5
                )
                logger.info("Database connection pool established")
                return
            except mysql.connector.Error as e:
                if attempt == max_retries - 1:
                    logger.error("Final database connection attempt failed")
                    raise
                logger.warning(f"Database connection failed (attempt {attempt+1}): {e}")
                time.sleep(2)

    def __init__(self):
        if not self._connection_pool:
            self.initialize_pool()
        self.connection = None

    # Get a connection from the pool
    def __enter__(self):
        self.connection = self._connection_pool.get_connection()
        return self

    # Return connection to the pool when done
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

# Execute user update operation in database
# SQL query to update user information in the database
    def execute_update(self, user_data):
        try:
            with self.connection.cursor() as cursor:
                query = """
                UPDATE client SET
                    first_name = %s,
                    last_name = %s,
                    email = %s,
                    phone = %s,
                    company = %s,
                    company_vat = %s,
                    address_1 = %s
                WHERE id = %s
                """
                params = (
                    user_data.get('first_name', ''),
                    user_data.get('last_name', ''),
                    user_data.get('email', ''),
                    user_data.get('phone', ''),
                    user_data.get('business_name', ''),
                    user_data.get('btw_number', ''),
                    user_data.get('address', ''),
                    user_data['id']
                )
                cursor.execute(query, params)
                self.connection.commit()
                return True
        except mysql.connector.Error as e:
            self.connection.rollback()
            logger.error(f"Database error updating user {user_data.get('id')}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

class MessageParser:
    # Handles parsing of both XML and JSON messages for user updates in RabbitMQ queue
    # Parse incoming message based on content type (XML or JSON)
    @staticmethod
    def parse(body, content_type):
        try:
            if content_type == "application/xml":
                return MessageParser._parse_xml(body)
            return MessageParser._parse_json(body)
        except Exception as e:
            logger.error(f"Parsing failed: {e}")
            raise

    # Parse XML message and extract user data
    @staticmethod
    def _parse_xml(xml_string):
        root = safe_xml_parse(xml_string)
        return {
            'id': int(root.findtext('UserID', '0')),
            'email': root.findtext('EmailAddress', '').strip(),
            'first_name': root.findtext('FirstName', '').strip(),
            'last_name': root.findtext('LastName', '').strip(),
            'phone': root.findtext('PhoneNumber', '').strip(),
            'business_name': (root.findtext('Business/BusinessName') or '').strip(),
            'btw_number': (root.findtext('Business/BTWNumber') or '').strip(),
            'address': (root.findtext('Business/Address') or '').strip()
        }

    # Parse JSON message and extract user data from JSON string
    @staticmethod
    def _parse_json(json_string):
        data = json.loads(json_string)
        return {
            'id': int(data.get('id', 0)),
            'email': data.get('email', '').strip(),
            'first_name': data.get('first_name', '').strip(),
            'last_name': data.get('last_name', '').strip(),
            'phone': data.get('phone', '').strip(),
            'business_name': data.get('business_name', '').strip(),
            'btw_number': data.get('btw_number', '').strip(),
            'address': data.get('address', '').strip()
        }

# Handles core business logic for user updates
class UserService:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.parser = MessageParser()

  # Validate incoming user data for required fields and data types
    def validate_user_data(self, data):
        required_fields = ['id', 'email', 'first_name', 'last_name']
        
        if not all(field in data for field in required_fields):
            raise ValueError(f"Missing required fields: {required_fields}")
        
        if not isinstance(data['id'], int) or data['id'] <= 0:
            raise ValueError("Invalid user ID")
            
        if '@' not in data['email']:
            raise ValueError("Invalid email format")

    # Process incoming user update message and update database with new user data 
    def process_update(self, body, content_type):
        try:
            user_data = self.parser.parse(body, content_type)
            self.validate_user_data(user_data)
            
            with self.db_manager as db:
                return db.execute_update(user_data)
                
        except ValueError as e:
            logger.error(f"Validation error: {e}")
            return False
        except Exception as e:
            logger.error(f"Processing error: {e}")
            return False

# Handles RabbitMQ connection and message consumption
class RabbitMQConsumer:
    def __init__(self):
        self.user_service = UserService()
        self.connection = None
        self.channel = None
        self.queues = ['kassa_user_delete', 'crm_user_delete', 'frontend_user_delete']

 # Establish connection to RabbitMQ server and set up required queues for message consumption
    def connect(self):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=os.getenv('RABBITMQ_HOST'),
                        port=int(os.getenv('RABBITMQ_PORT')),
                        credentials=pika.PlainCredentials(
                            os.getenv('RABBITMQ_USER'),
                            os.getenv('RABBITMQ_PASSWORD')
                        ),
                        heartbeat=600,
                        connection_attempts=3
                    )
                )
                self.channel = self.connection.channel()
                
                # Set up all required queues for message consumption (if they don't exist) 
                for queue in self.queues:
                    self.channel.queue_declare(queue=queue, durable=True)
                
                logger.info("RabbitMQ connection established")
                return True
                
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error("Final RabbitMQ connection attempt failed")
                    raise
                logger.warning(f"RabbitMQ connection failed (attempt {attempt+1}): {e}")
                time.sleep(2)

 # Callback function that processes each incoming message
    def _process_message(self, ch, method, properties, body):

        try:
            content_type = properties.content_type or "application/json"
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Received message: {body[:200]}...")
            
            if self.user_service.process_update(body.decode(), content_type):
                ch.basic_ack(method.delivery_tag)
                logger.info(f"Processed message from {method.routing_key}")
            else:
                ch.basic_nack(method.delivery_tag, requeue=False)
                logger.error(f"Failed to process message from {method.routing_key}")
                
        except Exception as e:
            logger.error(f"Message processing failed: {e}")
            ch.basic_nack(method.delivery_tag, requeue=False)

    def start_consuming(self):
        # Start listening for messages on all queues and process each message as it arrives 
        try:
            for queue in self.queues:
                self.channel.basic_consume(
                    queue=queue,
                    on_message_callback=self._process_message,
                    auto_ack=False
                )
            
            logger.info("Starting message consumer...")
            self.channel.start_consuming()
            
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            raise

    def shutdown(self):
        # Clean up RabbitMQ connection 
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
            if self.connection and self.connection.is_open:
                self.connection.close()
            logger.info("RabbitMQ connection closed")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

def main():
    consumer = None
    try:
        DatabaseManager.initialize_pool()
        consumer = RabbitMQConsumer()
        if consumer.connect():
            consumer.start_consuming()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        if consumer:
            consumer.shutdown()
        logger.info("Service shutdown complete")

if __name__ == "__main__":
    main()