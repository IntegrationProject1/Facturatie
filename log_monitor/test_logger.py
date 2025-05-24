import unittest
from unittest.mock import patch, MagicMock
import xml.etree.ElementTree as ET
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

os.environ["RABBITMQ_HOST"] = "localhost"
os.environ["RABBITMQ_PORT"] = "5672"
os.environ["RABBITMQ_USER"] = "guest"
os.environ["RABBITMQ_PASSWORD"] = "guest"

from logger import create_xml_log, publish_log, monitor_container_logs

class TestLogger(unittest.TestCase):

    def test_create_xml_log(self):
        xml_output = create_xml_log("ERROR", "Something went wrong", "facturatie_app")
        root = ET.fromstring(xml_output)
        
        self.assertEqual(root.find("ServiceName").text, "Facturatie::facturatie_app")
        self.assertEqual(root.find("Status").text, "ERROR")
        self.assertEqual(root.find("Message").text, "Something went wrong")

    @patch("logger.pika.BlockingConnection")
    def test_publish_log_success(self, mock_connection):
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel

        xml_message = create_xml_log("INFO", "Test message")
        publish_log(xml_message)

        mock_channel.exchange_declare.assert_called_with(exchange="log_monitoring", exchange_type="direct", durable=True)
        mock_channel.basic_publish.assert_called_with(
            exchange="log_monitoring",
            routing_key="controlroom.log.event",
            body=xml_message
        )

    @patch("logger.publish_log")
    def test_monitor_container_logs_detects_errors_and_warnings(self, mock_publish):
        mock_container = MagicMock()
        mock_container.name = "facturatie_app"
        mock_container.logs.return_value = iter([
            b"This is a normal log",
            b"Warning: deprecated method",
            b"Critical failure occurred!"
        ])

        monitor_container_logs(mock_container)

        calls = [call[0][0] for call in mock_publish.call_args_list]
        decoded = [ET.fromstring(c) for c in calls]

        self.assertEqual(decoded[0].find("Status").text, "INFO")
        self.assertEqual(decoded[1].find("Status").text, "WARNING")
        self.assertEqual(decoded[2].find("Status").text, "ERROR")

if __name__ == "__main__":
    unittest.main()
