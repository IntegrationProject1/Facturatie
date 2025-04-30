import unittest
from unittest.mock import patch, MagicMock
import xml.etree.ElementTree as ET
import logging
import io
import os

from user_creation_consumer import (
    user_exists,
    parse_user_xml,
    create_user,
    on_message,
    start_consumer
)

class TestUserCreationConsumer(unittest.TestCase):
    
    def setUp(self):
        # Set up test data
        self.sample_xml = """
        <UserMessage>
            <ActionType>CREATE</ActionType>
            <UUID>2025-04-29T14:22:27.816332Z</UUID>
            <TimeOfAction>2025-04-29T14:22:27.816332Z</TimeOfAction>
            <EncryptedPassword>secure123</EncryptedPassword>
            <FirstName>John</FirstName>
            <LastName>Doe</LastName>
            <PhoneNumber>+1234567890</PhoneNumber>
            <EmailAddress>john.doe@example.com</EmailAddress>
            <Business>
                <BusinessName>Test Corp</BusinessName>
                <BusinessEmail>business@test.com</BusinessEmail>
                <RealAddress>123 Test St</RealAddress>
                <BTWNumber>BE0123456789</BTWNumber>
                <FacturationAddress>Same as real</FacturationAddress>
            </Business>
        </UserMessage>
        """
        
        self.sample_data = {
            'action_type': 'CREATE',
            'uuid': '2025-04-29T14:22:27.816332Z',
            'timestamp': '2025-04-29T14:22:27.816332Z',
            'password': 'secure123',
            'first_name': 'John',
            'last_name': 'Doe',
            'phone': '+1234567890',
            'email': 'john.doe@example.com',
            'company': 'Test Corp',
            'company_email': 'business@test.com',
            'address': '123 Test St',
            'vat': 'BE0123456789',
            'invoice_address': 'Same as real'
        }
        
        # Proper log capture setup
        self.log_capture = io.StringIO()
        handler = logging.StreamHandler(self.log_capture)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        logger = logging.getLogger('user_creation_consumer')
        logger.handlers = []
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    def tearDown(self):
        self.log_capture.close()

    @patch('user_creation_consumer.mysql.connector.connect')
    def test_user_exists_true(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = [1]
        
        result = user_exists('2025-04-29T14:22:27.816332Z')
        self.assertTrue(result)
        
    @patch('user_creation_consumer.mysql.connector.connect')
    def test_user_exists_false(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None
        
        result = user_exists('2025-04-29T14:22:27.816332Z')
        self.assertFalse(result)
        
    def test_parse_user_xml_success(self):
        result = parse_user_xml(self.sample_xml)
        self.assertEqual(result['action_type'], 'CREATE')
        self.assertEqual(result['email'], 'john.doe@example.com')
        self.assertEqual(result['vat'], 'BE0123456789')
        
    def test_parse_user_xml_missing_fields(self):
        test_xml = """
        <UserMessage>
            <ActionType>CREATE</ActionType>
            <UUID>2025-04-29T14:22:27.816332Z</UUID>
            <TimeOfAction>2025-04-29T14:22:27.816332Z</TimeOfAction>
            <EncryptedPassword>secure123</EncryptedPassword>
        </UserMessage>
        """
        result = parse_user_xml(test_xml)
        self.assertEqual(result['first_name'], '')  # Default empty string
        self.assertEqual(result['company'], '')    # Default empty string
        
    def test_parse_user_xml_invalid(self):
        with self.assertRaises(Exception):
            parse_user_xml("invalid xml data")

    @patch('user_creation_consumer.user_exists')
    @patch('user_creation_consumer.mysql.connector.connect')
    def test_create_user_success(self, mock_connect, mock_user_exists):
        mock_user_exists.return_value = False
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        result = create_user(self.sample_data)
        self.assertTrue(result)
        mock_cursor.execute.assert_called_once()
        
    @patch('user_creation_consumer.user_exists')
    def test_create_user_already_exists(self, mock_user_exists):
        mock_user_exists.return_value = True
        
        result = create_user(self.sample_data)
        self.assertFalse(result)
        logs = self.log_capture.getvalue()
        self.assertIn("User met timestamp", logs)
        self.assertIn("bestaat al", logs)

    @patch('user_creation_consumer.create_user')
    @patch('user_creation_consumer.parse_user_xml')
    def test_on_message_success(self, mock_parse, mock_create):
        mock_parse.return_value = self.sample_data
        mock_create.return_value = True
        
        mock_channel = MagicMock()
        mock_method = MagicMock()
        
        on_message(mock_channel, mock_method, None, self.sample_xml.encode())
        
        # Verify UUID formatting was applied
        parsed_data = mock_parse.return_value
        self.assertNotIn('Z', parsed_data['uuid'])
        self.assertNotIn('T', parsed_data['uuid'])
        
        mock_channel.basic_ack.assert_called_once_with(mock_method.delivery_tag)

    @patch('user_creation_consumer.parse_user_xml')
    def test_on_message_wrong_action(self, mock_parse):
        test_data = self.sample_data.copy()
        test_data['action_type'] = 'UPDATE'
        mock_parse.return_value = test_data

        mock_channel = MagicMock()
        mock_method = MagicMock()

        on_message(mock_channel, mock_method, None, self.sample_xml.encode())

        logs = self.log_capture.getvalue()
        # Check for either straight or curly quotes
        self.assertTrue(
            "niet-'CREATE' actie" in logs or 
            "niet-‘CREATE’ actie" in logs,
            f"Expected warning message not found in logs: {logs}"
        )
        mock_channel.basic_ack.assert_called_once()

    @patch('user_creation_consumer.parse_user_xml')
    def test_on_message_failure(self, mock_parse):
        mock_parse.side_effect = Exception("Test error")
        
        mock_channel = MagicMock()
        mock_method = MagicMock()
        
        on_message(mock_channel, mock_method, None, self.sample_xml.encode())
        
        logs = self.log_capture.getvalue()
        self.assertIn("Test error", logs)
        mock_channel.basic_nack.assert_called_once_with(mock_method.delivery_tag, requeue=False)

    @patch.dict('os.environ', {
        'RABBITMQ_HOST': 'localhost',
        'RABBITMQ_PORT': '5672',
        'RABBITMQ_USER': 'guest',
        'RABBITMQ_PASSWORD': 'guest'
    })
    @patch('user_creation_consumer.pika.BlockingConnection')
    def test_start_consumer_normal_operation(self, mock_connection):
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel
        
        # Simulate keyboard interrupt
        mock_channel.start_consuming.side_effect = KeyboardInterrupt()
        
        start_consumer()
        
        logs = self.log_capture.getvalue()
        self.assertIn("Wachten op gebruikerscreatieberichten", logs)
        self.assertIn("Consumer stoppen", logs)

if __name__ == '__main__':
    unittest.main()