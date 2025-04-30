import unittest
from unittest.mock import patch, MagicMock
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from user_creation_providor import (
    get_db_connection,
    get_new_users,
    mark_as_processed,
    create_xml_message,
    send_to_rabbitmq,
    initialize_database
)
import logging
import io

class TestUserCreationProvidor(unittest.TestCase):
    
    def setUp(self):
        # Sample test data
        self.sample_user = {
            'id': 1,
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'pass': 'secure123',
            'phone': '1234567890',
            'created_at': '2023-01-01 12:00:00',
            'timestamp': '2023-01-01T12:00:00.000000Z',
            'business_name': 'Doe Inc',
            'btw_number': 'BE123456789',
            'real_address': '123 Main St, Brussels, Belgium'
        }
        
        # Configure log capture
        self.log_capture = io.StringIO()
        handler = logging.StreamHandler(self.log_capture)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        logger = logging.getLogger('user_creation_providor')
        logger.handlers = []
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    def tearDown(self):
        self.log_capture.close()

    @patch.dict('os.environ', {
        'DB_HOST': 'localhost',
        'DB_USER': 'test',
        'DB_PASSWORD': 'test',
        'DB_NAME': 'test_db'
    })
    @patch('user_creation_providor.mysql.connector.connect')
    def test_get_db_connection(self, mock_connect):
        get_db_connection()
        mock_connect.assert_called_once_with(
            host='localhost',
            user='test',
            password='test',
            database='test_db'
        )

    @patch('user_creation_providor.get_db_connection')
    def test_get_new_users(self, mock_db_conn):
        mock_cursor = MagicMock()
        mock_db_conn.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [self.sample_user]
        
        users = get_new_users()
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0]['first_name'], 'John')
        
        # Verify the query was correct
        mock_cursor.execute.assert_called_once()
        args = mock_cursor.execute.call_args[0][0]
        self.assertIn("WHERE p.client_id IS NULL", args)
        self.assertIn("ORDER BY c.created_at ASC", args)

    @patch('user_creation_providor.get_db_connection')
    def test_mark_as_processed(self, mock_db_conn):
        mock_cursor = MagicMock()
        mock_db_conn.return_value.cursor.return_value = mock_cursor

        mark_as_processed(123)

        # Get the actual SQL that was called
        actual_sql = mock_cursor.execute.call_args[0][0]
        
        # Verify the key components
        self.assertIn("INSERT INTO processed_users", actual_sql)
        self.assertIn("(client_id, processed_at)", actual_sql)
        self.assertIn("VALUES (%s, NOW())", actual_sql)
        
        # Verify the parameters
        self.assertEqual(mock_cursor.execute.call_args[0][1], (123,))
        
        # Verify commit was called
        mock_db_conn.return_value.commit.assert_called_once()

    def test_create_xml_message(self):
        xml_str = create_xml_message(self.sample_user)
        self.assertTrue(xml_str.startswith('<?xml version="1.0" encoding="UTF-8"?>'))
        
        # Parse the XML to verify structure
        root = ET.fromstring(xml_str.split('?>', 1)[1])
        self.assertEqual(root.find('ActionType').text, 'CREATE')
        self.assertEqual(root.find('FirstName').text, 'John')
        self.assertEqual(root.find('Business/BusinessName').text, 'Doe Inc')

    def test_create_xml_message_minimal(self):
        minimal_user = {
            'id': 2,
            'first_name': 'Jane',
            'email': 'jane@example.com',
            'pass': 'pass123',
            'created_at': '2023-01-01 12:00:00',
            'timestamp': '2023-01-01T12:00:00.000000Z'
        }
        
        xml_str = create_xml_message(minimal_user)
        root = ET.fromstring(xml_str.split('?>', 1)[1])
        
        # Should have basic fields
        self.assertEqual(root.find('FirstName').text, 'Jane')
        # Should not have business section
        self.assertIsNone(root.find('Business'))

    @patch.dict('os.environ', {
        'RABBITMQ_HOST': 'localhost',
        'RABBITMQ_PORT': '5672',
        'RABBITMQ_USER': 'guest',
        'RABBITMQ_PASSWORD': 'guest'
    })
    @patch('user_creation_providor.pika.BlockingConnection')
    def test_send_to_rabbitmq(self, mock_connection):
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel
        
        xml = create_xml_message(self.sample_user)
        result = send_to_rabbitmq(xml)
        
        self.assertTrue(result)
        # Should declare exchange once
        mock_channel.exchange_declare.assert_called_once_with(
            exchange="user",
            exchange_type="topic",
            durable=True
        )
        # Should send to 3 queues
        self.assertEqual(mock_channel.basic_publish.call_count, 3)
        
        # Verify logs
        logs = self.log_capture.getvalue()
        self.assertIn("Sent XML message to crm_user_create", logs)
        self.assertIn("Sent XML message to kassa_user_create", logs)
        self.assertIn("Sent XML message to frontend_user_create", logs)

    @patch('user_creation_providor.get_db_connection')
    def test_initialize_database(self, mock_db_conn):
        mock_cursor = MagicMock()
        mock_db_conn.return_value.cursor.return_value = mock_cursor

        initialize_database()

        # Get the actual SQL that was called
        actual_sql = mock_cursor.execute.call_args[0][0]
        
        # Verify the key components without worrying about whitespace
        self.assertIn("CREATE TABLE IF NOT EXISTS processed_users", actual_sql)
        self.assertIn("id INT AUTO_INCREMENT PRIMARY KEY", actual_sql)
        self.assertIn("client_id INT NOT NULL UNIQUE", actual_sql)
        self.assertIn("processed_at DATETIME NOT NULL", actual_sql)
        
        # Verify commit was called
        mock_db_conn.return_value.commit.assert_called_once()

if __name__ == '__main__':
    unittest.main()