import unittest
from unittest.mock import patch, MagicMock
import xml.etree.ElementTree as ET
import logging
import io
import os
from user_deletion_providor import (
    get_users_to_delete,
    mark_as_deleted,
    create_delete_xml,
    send_to_rabbitmq,
    initialize_database
)

class TestUserDeletionProvidor(unittest.TestCase):
    
    def setUp(self):
        # Sample test data
        self.sample_user = {
            'client_id': 1,
            'deleted_at': '2025-04-29 14:22:27.816332',
            'timestamp': '2025-04-29T14:22:27.816332Z'
        }
        
        # Configure log capture
        self.log_capture = io.StringIO()
        handler = logging.StreamHandler(self.log_capture)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        logger = logging.getLogger('user_deletion_provider')
        logger.handlers = []
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    def tearDown(self):
        self.log_capture.close()

    @patch('user_deletion_providor.get_db_connection')
    def test_get_users_to_delete_success(self, mock_get_db_connection):
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_get_db_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        mock_cursor.fetchall.return_value = [self.sample_user]
        
        result = get_users_to_delete()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['client_id'], 1)

    @patch('user_deletion_providor.get_db_connection')
    def test_mark_as_deleted_success(self, mock_get_db_connection):
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_get_db_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        mark_as_deleted(1)
        
        mock_cursor.execute.assert_called_once_with("""
            UPDATE user_deletion_notifications
            SET processed = 1
            WHERE client_id = %s
        """, (1,))
        mock_conn.commit.assert_called_once()

    def test_create_delete_xml_success(self):
        xml = create_delete_xml(self.sample_user)
        self.assertIn("<ActionType>DELETE</ActionType>", xml)
        self.assertIn("<UUID>2025-04-29T14:22:27.816332Z</UUID>", xml)

    @patch.dict('os.environ', {
        'RABBITMQ_HOST': 'localhost',
        'RABBITMQ_PORT': '5672',
        'RABBITMQ_USER': 'guest',
        'RABBITMQ_PASSWORD': 'guest'
    })
    @patch('user_deletion_providor.pika.BlockingConnection')
    def test_send_to_rabbitmq_success(self, mock_connection):
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel
        
        result = send_to_rabbitmq("<xml></xml>")
        self.assertTrue(result)
        mock_channel.basic_publish.assert_called()

    @patch('user_deletion_providor.pika.BlockingConnection')
    def test_send_to_rabbitmq_failure(self, mock_connection):
        mock_connection.side_effect = Exception("Connection error")
        
        result = send_to_rabbitmq("<xml></xml>")
        self.assertFalse(result)

    @patch('user_deletion_providor.get_db_connection')
    def test_initialize_database(self, mock_get_db_connection):
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_get_db_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        initialize_database()
        
        mock_cursor.execute.assert_called_once_with("""
            CREATE TABLE IF NOT EXISTS user_deletion_notifications (
                id INT AUTO_INCREMENT PRIMARY KEY,
                client_id INT NOT NULL,
                deleted_at DATETIME(6) NOT NULL,
                timestamp DATETIME(6),
                processed BOOLEAN DEFAULT 0
            )
        """)
        mock_conn.commit.assert_called_once()

if __name__ == '__main__':
    unittest.main()
