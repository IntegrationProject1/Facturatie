import pytest
import xml.etree.ElementTree as ET
from unittest.mock import patch, MagicMock, call
import os
import logging
import pika
from invoice_mailing_providor import (
    get_db_connection,
    get_invoices,
    mark_as_processed,
    create_xml_message,
    send_to_rabbitmq
)

# Setup logging for tests
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture
def sample_invoice_data():
    return [
        ('test@example.com', 'abc123', 1, 0, '2023-05-15 12:00:00', 1),
        ('another@test.com', 'def456', 1, 0, '2023-05-16 10:00:00', 2)
    ]

@pytest.fixture
def mock_db_connection():
    with patch('invoice_mailing_providor.mysql.connector.connect') as mock_connect:
        yield mock_connect

@pytest.fixture
def mock_rabbitmq():
    with patch('invoice_mailing_providor.pika.BlockingConnection') as mock_connection:
        yield mock_connection

@pytest.fixture
def env_vars():
    return {
        'DB_HOST': 'localhost',
        'DB_USER': 'test',
        'DB_PASSWORD': 'test',
        'DB_NAME': 'test_db',
        'INVOICE_HOST': 'example.com',
        'INVOICE_PORT': '8080',
        'RABBITMQ_HOST': 'localhost',
        'RABBITMQ_PORT': '5672',
        'RABBITMQ_USER': 'guest',
        'RABBITMQ_PASSWORD': 'guest'
    }

@pytest.fixture(autouse=True)
def setup_logging(caplog):
    caplog.set_level(logging.INFO)

def test_get_db_connection(mock_db_connection, env_vars):
    """Test database connection creation"""
    with patch.dict(os.environ, env_vars):
        get_db_connection()
    
    mock_db_connection.assert_called_once_with(
        host='localhost',
        user='test',
        password='test',
        database='test_db'
    )

def test_get_invoices_success(mock_db_connection, sample_invoice_data, env_vars):
    """Test successful invoice retrieval"""
    mock_cursor = MagicMock()
    mock_db_connection.return_value.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = sample_invoice_data
    
    with patch.dict(os.environ, env_vars):
        invoices = get_invoices()
    
    assert invoices == sample_invoice_data
    mock_cursor.execute.assert_called_once_with(
        """\n            SELECT \n                c.email, i.hash, i.approved, i.processed, i.created_at, i.id\n            FROM invoice i\n            JOIN client c ON i.client_id = c.id\n            WHERE i.processed = 0 AND i.approved = 1\n            ORDER BY i.created_at ASC\n        """
    )

def test_get_invoices_empty(mock_db_connection, env_vars, caplog):
    """Test invoice retrieval when no invoices found"""
    mock_cursor = MagicMock()
    mock_db_connection.return_value.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []
    
    with patch.dict(os.environ, env_vars):
        invoices = get_invoices()
    
    assert invoices == []
    assert any("No unprocessed invoices found" in record.message for record in caplog.records)

def test_get_invoices_error(mock_db_connection, env_vars, caplog):
    """Test invoice retrieval with database error"""
    mock_cursor = MagicMock()
    mock_db_connection.return_value.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = Exception("DB error")

    with patch.dict(os.environ, env_vars):
        invoices = get_invoices()

    assert invoices == []
    assert any("Error fetching invoices" in record.message for record in caplog.records)

def test_mark_as_processed_success(mock_db_connection, env_vars):
    """Test successful invoice marking"""
    mock_cursor = MagicMock()
    mock_db_connection.return_value.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (1,)  # approved = 1
    
    with patch.dict(os.environ, env_vars):
        mark_as_processed('abc123')
    
    calls = [
        call("""\n            SELECT approved FROM invoice WHERE hash = %s\n        """, ('abc123',)),
        call("""\n                UPDATE invoice\n                SET processed = 1\n                WHERE hash = %s\n            """, ('abc123',))
    ]
    mock_cursor.execute.assert_has_calls(calls)
    mock_db_connection.return_value.commit.assert_called_once()

def test_mark_as_processed_not_approved(mock_db_connection, env_vars):
    """Test marking when invoice not approved"""
    mock_cursor = MagicMock()
    mock_db_connection.return_value.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (0,)  # approved = 0
    
    with patch.dict(os.environ, env_vars):
        mark_as_processed('abc123')
    
    mock_cursor.execute.assert_called_once_with(
        """\n            SELECT approved FROM invoice WHERE hash = %s\n        """, ('abc123',)
    )
    mock_db_connection.return_value.commit.assert_not_called()

def test_mark_as_processed_error(mock_db_connection, env_vars, caplog):
    """Test marking with database error"""
    mock_cursor = MagicMock()
    mock_db_connection.return_value.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = Exception("DB error")

    with patch.dict(os.environ, env_vars):
        mark_as_processed('abc123')

    assert any("Error marking invoice as processed" in record.message for record in caplog.records)

def test_create_xml_message(env_vars):
    """Test XML message creation"""
    with patch.dict(os.environ, env_vars):
        xml = create_xml_message('test@example.com', 'abc123')
    
    root = ET.fromstring(xml)
    assert root.tag == 'emailMessage'
    assert root.attrib['service'] == 'facturatie'
    assert root.find('to').text == 'test@example.com'
    assert root.find('from').text == 'no.reply.expomail@gmail.com'
    assert root.find('subject').text == 'Invoice E-XPO'
    assert root.find('attachmenturl').text == 'http://example.com:8080/invoice/pdf/abc123'

def test_send_to_rabbitmq_success(mock_rabbitmq, env_vars):
    """Test successful RabbitMQ message sending"""
    with patch.dict(os.environ, env_vars):
        mock_channel = MagicMock()
        mock_rabbitmq.return_value.channel.return_value = mock_channel
        
        result = send_to_rabbitmq('<test>xml</test>')
        
        assert result is True
        mock_channel.exchange_declare.assert_called_once_with(
            exchange="email",
            exchange_type="topic",
            durable=True
        )
        mock_channel.queue_declare.assert_called_once_with(
            queue="mail_queue",
            durable=True
        )
        mock_channel.queue_bind.assert_called_once_with(
            exchange="email",
            queue="mail_queue",
            routing_key="mail"
        )
        mock_channel.basic_publish.assert_called_once_with(
            exchange="email",
            routing_key="mail",
            body='<test>xml</test>',
            properties=pika.BasicProperties(delivery_mode=2)
        )

def test_send_to_rabbitmq_failure(mock_rabbitmq, env_vars, caplog):
    """Test failed RabbitMQ message sending"""
    with patch.dict(os.environ, env_vars):
        mock_rabbitmq.side_effect = Exception("Connection failed")
        
        result = send_to_rabbitmq('<test>xml</test>')
        
        assert result is False
        assert any("RabbitMQ Error" in record.message for record in caplog.records)