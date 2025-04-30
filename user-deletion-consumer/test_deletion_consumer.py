import unittest
from unittest.mock import patch, MagicMock
import xml.etree.ElementTree as ET
import logging
import io
import os
from user_deletion_consumer import (
    user_exists,
    delete_user,
    parse_user_xml,
    on_message,
    start_consumer
)

class TestUserDeletionConsumer(unittest.TestCase):
    
    def setUp(self):
        # Sample test data
        self.sample_xml = """
        <UserMessage>
            <ActionType>DELETE</ActionType>
            <UUID>2025-04-29T14:22:27.816332Z</UUID>
            <TimeOfAction>2025-04-29T14:22:27.816332Z</TimeOfAction>
        </UserMessage>
        """
        
        self.sample_data = {
            'action_type': 'DELETE',
            'uuid': '2025-04-29T14:22:27.816332Z',
            'action_time': '2025-04-29T14:22:27.816332Z'
        }
        
        # Configure log capture
        self.log_capture = io.StringIO()
        handler = logging.StreamHandler(self.log_capture)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        logger = logging.getLogger('user_deletion_consumer')
        logger.handlers = []
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    def tearDown(self):
        self.log_capture.close()

    @patch('user_deletion_consumer.mysql.connector.connect')
    def test_user_exists_true(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = [1]  # User exists
        
        result = user_exists('2025-04-29T14:22:27.816332Z')
        self.assertTrue(result)

    @patch('user_deletion_consumer.mysql.connector.connect')
    def test_user_exists_false(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None  # User doesn't exist
        
        result = user_exists('2025-04-29T14:22:27.816332Z')
        self.assertFalse(result)

    @patch('user_deletion_consumer.user_exists')
    @patch('user_deletion_consumer.mysql.connector.connect')
    def test_delete_user_success(self, mock_connect, mock_user_exists):
        mock_user_exists.return_value = True
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        result = delete_user(self.sample_data)
        self.assertTrue(result)
        logs = self.log_capture.getvalue()
        self.assertIn("Deleted client", logs)

    @patch('user_deletion_consumer.mysql.connector.connect')
    @patch('user_deletion_consumer.user_exists')
    def test_delete_user_not_found(self, mock_user_exists, mock_connect):
        mock_user_exists.return_value = False
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        result = delete_user(self.sample_data)

        self.assertEqual(result, False)
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_parse_user_xml_success(self):
        result = parse_user_xml(self.sample_xml)
        self.assertEqual(result['action_type'], 'DELETE')
        self.assertEqual(result['uuid'], '2025-04-29T14:22:27.816332Z')

    def test_parse_user_xml_invalid(self):
        with self.assertRaises(Exception):
            parse_user_xml("invalid xml")

    @patch('user_deletion_consumer.delete_user')
    @patch('user_deletion_consumer.parse_user_xml')
    def test_on_message_success(self, mock_parse, mock_delete):
        mock_parse.return_value = self.sample_data
        mock_delete.return_value = True
        
        mock_channel = MagicMock()
        mock_method = MagicMock()
        
        on_message(mock_channel, mock_method, None, self.sample_xml.encode())
        
        # Verify UUID formatting
        parsed_data = mock_parse.return_value
        self.assertNotIn('Z', parsed_data['uuid'])
        self.assertNotIn('T', parsed_data['uuid'])
        
        mock_channel.basic_ack.assert_called_once()

    @patch('user_deletion_consumer.parse_user_xml')
    def test_on_message_wrong_action(self, mock_parse):
        test_data = self.sample_data.copy()
        test_data['action_type'] = 'CREATE'
        mock_parse.return_value = test_data
        
        mock_channel = MagicMock()
        mock_method = MagicMock()
        
        on_message(mock_channel, mock_method, None, self.sample_xml.encode())
        
        logs = self.log_capture.getvalue()
        self.assertIn("non-DELETE action", logs)
        mock_channel.basic_ack.assert_called_once()

    @patch('user_deletion_consumer.parse_user_xml')
    def test_on_message_failure(self, mock_parse):
        mock_parse.side_effect = Exception("Test error")

        mock_channel = MagicMock()
        mock_method = MagicMock()
        mock_method.delivery_tag = 123  # Add this line to provide a delivery tag

        on_message(mock_channel, mock_method, None, self.sample_xml.encode())

        logs = self.log_capture.getvalue()
        self.assertIn("Test error", logs)
        mock_channel.basic_nack.assert_called_once_with(123, requeue=False)

    @patch.dict('os.environ', {
        'RABBITMQ_HOST': 'localhost',
        'RABBITMQ_PORT': '5672',
        'RABBITMQ_USER': 'guest',
        'RABBITMQ_PASSWORD': 'guest'
    })
    @patch('user_deletion_consumer.pika.BlockingConnection')
    def test_start_consumer(self, mock_connection):
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel
        mock_channel.start_consuming.side_effect = KeyboardInterrupt()
        
        start_consumer()
        
        logs = self.log_capture.getvalue()
        self.assertIn("Waiting for user deletion messages", logs)
        self.assertIn("Stopping consumer", logs)

if __name__ == '__main__':
    unittest.main()