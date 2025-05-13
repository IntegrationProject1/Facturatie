import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import json

from invoice_creator import create_invoice
from rabbitmq_publisher import send_to_mailing_queue
from order_consumer import process_order

class TestInvoiceProcessor(unittest.TestCase):

    @patch('invoice_creator.mysql.connector.connect')
    def test_create_invoice(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.lastrowid = 101

        order_data = {
            "date": "2024-05-06T10:00:00",
            "uuid": "2024-05-06T10:00:00",
            "products": [
                {"product_nr": 1, "quantity": 2, "unit_price": 15.0},
                {"product_nr": 2, "quantity": 1, "unit_price": 20.0}
            ]
        }

        invoice = create_invoice(order_data)

        self.assertEqual(invoice['total_amount'], 50.0)
        self.assertEqual(invoice['id'], 101)
        self.assertIn("INSERT INTO invoices", mock_cursor.execute.call_args_list[0][0][0])
        self.assertIn("INSERT INTO invoice_items", mock_cursor.execute.call_args_list[1][0][0])
        mock_conn.commit.assert_called()
        mock_cursor.close.assert_called()
        mock_conn.close.assert_called()

    @patch('rabbitmq_publisher.pika.BlockingConnection')
    def test_send_to_mailing_queue(self, mock_connection):
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel

        invoice = {
            "id": 123,
            "client_id": 1,
            "uuid": "2024-05-06T10:00:00",
            "total_amount": 50.0,
            "currency": "USD",
            "status": "paid"
        }

        send_to_mailing_queue(invoice)

        mock_channel.queue_declare.assert_called_once_with(queue='mail_queue', durable=True)
        mock_channel.basic_publish.assert_called_once()
        args, kwargs = mock_channel.basic_publish.call_args
        self.assertEqual(kwargs['routing_key'], 'mail_queue')
        self.assertIn('"id": 123', kwargs['body'])
        mock_connection.return_value.close.assert_called_once()

    @patch('invoice_creator.mysql.connector.connect')
    @patch('rabbitmq_publisher.pika.BlockingConnection')
    def test_full_order_processing_flow(self, mock_pika_conn, mock_mysql_conn):
        test_xml = """<Order>
            <Date>2024-05-11T14:30:00</Date>
            <UUID>2024-05-11T14:31:00</UUID>
            <Products>
                <Product>
                    <ProductNR>101</ProductNR>
                    <Quantity>2</Quantity>
                    <UnitPrice>25.00</UnitPrice>
                </Product>
            </Products>
        </Order>"""
        xsd_path = "order.xsd"  # Ensure this file exists for real validation

        # Mock MySQL
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_mysql_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.lastrowid = 999

        # Mock RabbitMQ
        mock_channel = MagicMock()
        mock_pika_conn.return_value.channel.return_value = mock_channel

        # Call the full function
        process_order(test_xml, xsd_path)

        # MySQL checks
        self.assertIn("INSERT INTO invoices", mock_cursor.execute.call_args_list[0][0][0])
        self.assertIn("INSERT INTO invoice_items", mock_cursor.execute.call_args_list[1][0][0])
        mock_conn.commit.assert_called()

        # RabbitMQ checks
        mock_channel.queue_declare.assert_called_with(queue='mail_queue', durable=True)
        mock_channel.basic_publish.assert_called_once()

        args, kwargs = mock_channel.basic_publish.call_args
        body = kwargs.get('body') or args[2]
        self.assertIn('"id": 999', body)

if __name__ == '__main__':
    unittest.main()
