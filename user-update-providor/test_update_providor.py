import unittest
from unittest.mock import patch, MagicMock
import xml.etree.ElementTree as ET
import logging
import io
import os
from user_update_providor import (
    get_updated_users,
    mark_as_processed,
    create_xml_message,
    send_to_rabbitmq,
    initialize_database
)

class TestUserUpdateProvidor(unittest.TestCase):
    
    def setUp(self):
        # Sample test data
        self.sample_user = {
            'id': 1,
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john.doe@example.com',
            'pass': 'encrypted_password',
            'phone': '1234567890',
            'business_name': 'Doe Enterprises',
            'btw_number': 'VAT123456',
            'real_address': '123 Main St, Anytown, USA',
            'updated_at': '2025-04-29 14:22:27',
            'timestamp': '2025-04-29T14:22:27.000000Z'
        }
        
        # Configure log capture
        self.log_capture = io.StringIO()
        handler = logging.StreamHandler(self.log_capture)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        logger = logging.getLogger('user_update_providor')
        logger.handlers = []
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    def tearDown(self):
        self.log_capture.close()

    @patch('user_update_providor.get_db_connection')
    def test_get_updated_users_success(self, mock_get_db_connection):
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_get_db_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        mock_cursor.fetchall.return_value = [self.sample_user]
        
        result = get_updated_users()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['id'], 1)

    @patch('user_update_providor.get_db_connection')
    def test_mark_as_processed_success(self, mock_get_db_connection):
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_get_db_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        mark_as_processed(1)
        
        mock_cursor.execute.assert_called_once_with("""
            UPDATE user_updates_queue
            SET processed = TRUE
            WHERE client_id = %s
        """, (1,))
        mock_conn.commit.assert_called_once()

    def test_create_xml_message_success(self):
        xml = create_xml_message(self.sample_user)
        self.assertIn("<ActionType>UPDATE</ActionType>", xml)
        self.assertIn("<UUID>2025-04-29T14:22:27.000000Z</UUID>", xml)
        self.assertIn("<FirstName>John</FirstName>", xml)
        self.assertIn("<LastName>Doe</LastName>", xml)

    @patch.dict('os.environ', {
        'RABBITMQ_HOST': 'localhost',
        'RABBITMQ_PORT': '5672',
        'RABBITMQ_USER': 'guest',
        'RABBITMQ_PASSWORD': 'guest'
    })
    @patch('user_update_providor.pika.BlockingConnection')
    def test_send_to_rabbitmq_success(self, mock_connection):
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel
        
        result = send_to_rabbitmq("<xml></xml>")
        self.assertTrue(result)
        mock_channel.basic_publish.assert_called()

    @patch('user_update_providor.pika.BlockingConnection')
    def test_send_to_rabbitmq_failure(self, mock_connection):
        mock_connection.side_effect = Exception("Connection error")
        
        result = send_to_rabbitmq("<xml></xml>")
        self.assertFalse(result)

    @patch('user_update_providor.get_db_connection')
    def test_initialize_database(self, mock_get_db_connection):
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_get_db_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        initialize_database()
        
        mock_cursor.execute.assert_called_once_with("""
            CREATE TABLE IF NOT EXISTS user_updates_queue (
                id INT AUTO_INCREMENT PRIMARY KEY,
                client_id INT NOT NULL UNIQUE,
                updated_at DATETIME(6) NOT NULL,
                processed BOOLEAN DEFAULT FALSE,
                INDEX (client_id),
                INDEX (processed)
            )
        """)
        mock_conn.commit.assert_called_once()

if __name__ == '__main__':
    unittest.main()
