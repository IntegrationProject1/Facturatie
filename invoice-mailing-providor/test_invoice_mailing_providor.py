import unittest
from unittest.mock import patch, MagicMock
import os
import xml.etree.ElementTree as ET
import logging
import io

from invoice_mailing_providor import (
    get_invoices,
    mark_as_processed,
    create_xml_message,
    send_to_rabbitmq
)


class TestInvoiceProcessor(unittest.TestCase):

    def setUp(self):
        # Sample test data
        self.sample_invoice = [
            ("john.doe@example.com", "abc123", 1, 0, "2024-01-01 10:00:00", 101)
        ]

        # Configure log capture
        self.log_capture = io.StringIO()
        handler = logging.StreamHandler(self.log_capture)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        logger = logging.getLogger('invoice_processor')
        logger.handlers = []
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        # Patch environment variables
        self.env_patch = patch.dict(os.environ, {
            "DB_HOST": "localhost",
            "DB_USER": "user",
            "DB_PASSWORD": "pass",
            "DB_NAME": "testdb",
            "INVOICE_HOST": "localhost",
            "INVOICE_PORT": "8080",
            "RABBITMQ_HOST": "localhost",
            "RABBITMQ_PORT": "5672",
            "RABBITMQ_USER": "guest",
            "RABBITMQ_PASSWORD": "guest",
        })
        self.env_patch.start()

    def tearDown(self):
        self.env_patch.stop()
        self.log_capture.close()

    @patch('invoice_mailing_providor.mysql.connector.connect')
    def test_get_invoices_returns_invoices(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = self.sample_invoice

        invoices = get_invoices()

        self.assertEqual(len(invoices), 1)
        self.assertEqual(invoices[0][0], "john.doe@example.com")
        self.assertEqual(invoices[0][1], "abc123")

    @patch('invoice_mailing_providor.mysql.connector.connect')
    def test_get_invoices_empty(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        invoices = get_invoices()

        self.assertEqual(invoices, [])
        logs = self.log_capture.getvalue()
        self.assertIn("No unprocessed invoices found.", logs)

    @patch('invoice_mailing_providor.mysql.connector.connect')
    def test_mark_as_processed_success(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (1,)

        mark_as_processed("abc123")

        sql_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        self.assertIn("SELECT approved FROM invoice WHERE hash = %s", sql_calls[0])
        self.assertIn("UPDATE invoice", sql_calls[1])

    def test_create_xml_message_correctness(self):
        xml_str = create_xml_message("client@example.com", "abc123")
        root = ET.fromstring(xml_str)

        self.assertEqual(root.find("to").text, "client@example.com")
        self.assertEqual(root.find("subject").text, "Invoice E-XPO")
        self.assertIn("abc123", root.find("body").text)

    @patch('invoice_mailing_providor.pika.BlockingConnection')
    def test_send_to_rabbitmq_success(self, mock_connection):
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel

        result = send_to_rabbitmq("<xml>test</xml>")

        self.assertTrue(result)
        mock_channel.basic_publish.assert_called()

    @patch('invoice_mailing_providor.pika.BlockingConnection', side_effect=Exception("fail"))
    def test_send_to_rabbitmq_failure(self, mock_connection):
        result = send_to_rabbitmq("<xml>test</xml>")
        self.assertFalse(result)

        logs = self.log_capture.getvalue()
        self.assertIn("RabbitMQ Error", logs)


if __name__ == '__main__':
    unittest.main()
