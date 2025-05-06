import unittest
from unittest.mock import patch, MagicMock
import xml.etree.ElementTree as ET
import logging
import io
import os
from user_update_consumer import (
    get_current_user_data,
    update_user,
    parse_user_xml,
    on_message,
    start_consumer
)

class TestUserUpdateConsumer(unittest.TestCase):
    
    def setUp(self):
        # Sample test data
        self.sample_xml = """
        <UserMessage>
            <ActionType>UPDATE</ActionType>
            <UUID>2025-04-29T14:22:27.816332Z</UUID>
            <TimeOfAction>2025-04-29T14:22:27.816332Z</TimeOfAction>
            <FirstName>John</FirstName>
            <LastName>Doe</LastName>
            <EmailAddress>johndoe@example.com</EmailAddress>
            <PhoneNumber>1234567890</PhoneNumber>
            <EncryptedPassword>newpassword</EncryptedPassword>
        </UserMessage>
        """
        
        self.sample_data = {
            'action_type': 'UPDATE',
            'uuid': '2025-04-29T14:22:27.816332Z',
            'timestamp': '2025-04-29T14:22:27.816332Z',
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'johndoe@example.com',
            'phone': '1234567890',
            'password': 'newpassword',
            'company': None,
            'company_email': None,
            'address': None,
            'vat': None,
            'invoice_address': None
        }
        
        # Configure log capture
        self.log_capture = io.StringIO()
        handler = logging.StreamHandler(self.log_capture)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        logger = logging.getLogger('user_update_consumer')
        logger.handlers = []
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    def tearDown(self):
        self.log_capture.close()

    @patch('user_update_consumer.mysql.connector.connect')
    def test_get_current_user_data_exists(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = [1]  # User exists
        
        result = get_current_user_data('2025-04-29T14:22:27.816332Z')
        self.assertIsNotNone(result)

    @patch('user_update_consumer.mysql.connector.connect')
    def test_get_current_user_data_not_found(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None  # User doesn't exist
        
        result = get_current_user_data('2025-04-29T14:22:27.816332Z')
        self.assertIsNone(result)

    @patch('user_update_consumer.mysql.connector.connect')
    def test_update_user_success(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = [1]  # User exists
        
        result = update_user(self.sample_data)
        self.assertTrue(result)
        logs = self.log_capture.getvalue()
        self.assertIn("Successfully updated user", logs)

    @patch('user_update_consumer.mysql.connector.connect')
    def test_update_user_not_found(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None  # User doesn't exist
        
        result = update_user(self.sample_data)
        self.assertFalse(result)
        logs = self.log_capture.getvalue()
        self.assertIn("Cannot update - user with timestamp", logs)

    def test_parse_user_xml_success(self):
        result = parse_user_xml(self.sample_xml)
        self.assertEqual(result['action_type'], 'UPDATE')
        self.assertEqual(result['uuid'], '2025-04-29T14:22:27.816332Z')

    def test_parse_user_xml_invalid(self):
        with self.assertRaises(Exception):
            parse_user_xml("invalid xml")

    @patch('user_update_consumer.update_user')
    @patch('user_update_consumer.parse_user_xml')
    def test_on_message_success(self, mock_parse, mock_update):
        mock_parse.return_value = self.sample_data
        mock_update.return_value = True
        
        mock_channel = MagicMock()
        mock_method = MagicMock()
        
        on_message(mock_channel, mock_method, None, self.sample_xml.encode())
        
        # Verify UUID formatting
        parsed_data = mock_parse.return_value
        self.assertNotIn('Z', parsed_data['uuid'])
        self.assertNotIn('T', parsed_data['uuid'])
        
        mock_channel.basic_ack.assert_called_once()

    @patch('user_update_consumer.update_user')
    @patch('user_update_consumer.parse_user_xml')
    def test_on_message_wrong_action(self, mock_parse, mock_update):
        test_data = self.sample_data.copy()
        test_data['action_type'] = 'DELETE'
        mock_parse.return_value = test_data
        
        mock_channel = MagicMock()
        mock_method = MagicMock()

        on_message(mock_channel, mock_method, None, self.sample_xml.encode())

        logs = self.log_capture.getvalue()
        # Verify we logged about ignoring the action
        self.assertIn("Ignoring non-UPDATE action: DELETE", logs)
        # Verify we acknowledged the message
        mock_channel.basic_ack.assert_called_once()
        # Verify we didn't try to update
        mock_update.assert_not_called()  

    @patch('user_update_consumer.parse_user_xml')
    def test_on_message_failure(self, mock_parse):
        mock_parse.side_effect = Exception("Test error")

        mock_channel = MagicMock()
        mock_method = MagicMock()
        mock_method.delivery_tag = 123

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
    @patch('user_update_consumer.pika.BlockingConnection')
    def test_start_consumer(self, mock_connection):
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel
        mock_channel.start_consuming.side_effect = KeyboardInterrupt()
        
        start_consumer()
        
        logs = self.log_capture.getvalue()
        self.assertIn("Waiting for user update messages", logs)
        self.assertIn("Stopping consumer", logs)

if __name__ == '__main__':
    unittest.main()
