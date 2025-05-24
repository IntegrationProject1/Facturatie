import pytest
import hashlib
import json
import xml.etree.ElementTree as ET
from unittest.mock import patch, MagicMock
import os
import logging
from invoice_kassa_consumer import ( 
    parse_invoice_xml,
    generate_invoice_hash,
    get_client_by_uuid,
    create_invoice,
    on_message,
    start_consumer
)

# Setup logging for tests
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture
def sample_xml_data():
    return """<?xml version="1.0" encoding="UTF-8"?>
<Invoice>
    <UUID>123e4567-e89b-12d3-a456-426614174000</UUID>
    <Date>2023-05-15T12:00:00Z</Date>
    <Products>
        <Product>
            <ProductNR>P001</ProductNR>
            <Quantity>2</Quantity>
            <UnitPrice>19.99</UnitPrice>
            <ProductNaam>Test Product</ProductNaam>
        </Product>
    </Products>
</Invoice>"""

@pytest.fixture
def sample_invoice_data():
    return {
        'uuid': '123e4567-e89b-12d3-a456-426614174000',
        'date': '2023-05-15T12:00:00',
        'products': [{
            'product_id': 'P001',
            'quantity': '2',
            'price': '19.99',
            'name': 'Test Product'
        }]
    }

@pytest.fixture
def sample_client_info():
    return {
        'id': 1,
        'email': 'test@example.com',
        'first_name': 'John',
        'last_name': 'Doe',
        'phone_cc': '+1',
        'phone': '555-1234',
        'company': 'Test Corp',
        'company_vat': 'BE123456789',
        'company_number': '123456789',
        'city': 'Testville',
        'state': 'TS',
        'postcode': '12345',
        'country': 'Testland',
        'currency': 'USD',
        'address_1': '123 Test St'
    }

def test_parse_invoice_xml(sample_xml_data):
    """Test parsing of valid XML data"""
    result = parse_invoice_xml(sample_xml_data)
    
    assert result['uuid'] == '123e4567-e89b-12d3-a456-426614174000'
    assert result['date'] == '2023-05-15T12:00:00'
    assert len(result['products']) == 1
    assert result['products'][0]['product_id'] == 'P001'
    assert result['products'][0]['quantity'] == '2'
    assert result['products'][0]['price'] == '19.99'
    assert result['products'][0]['name'] == 'Test Product'

def test_parse_invoice_xml_invalid():
    """Test parsing of invalid XML data"""
    with pytest.raises(Exception):
        parse_invoice_xml("Invalid XML")

def test_generate_invoice_hash(sample_invoice_data):
    """Test invoice hash generation"""
    hash_result = generate_invoice_hash(sample_invoice_data)
    
    # Verify the hash is generated correctly
    hash_input = {
        'uuid': sample_invoice_data['uuid'],
        'products': sample_invoice_data['products'],
        'date': sample_invoice_data['date'],
    }
    expected_hash = hashlib.sha256(
        json.dumps(hash_input, sort_keys=True).encode('utf-8')
    )
    assert hash_result == expected_hash.hexdigest()

@patch('invoice_kassa_consumer.mysql.connector.connect')
def test_get_client_by_uuid_success(mock_connect, sample_client_info):
    """Test successful client lookup by UUID"""
    # Setup mock database connection
    mock_cursor = MagicMock()
    mock_connect.return_value.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = tuple(sample_client_info.values())
    
    uuid = '123e4567-e89b-12d3-a456-426614174000'
    result = get_client_by_uuid(uuid)
    
    assert result == sample_client_info
    mock_cursor.execute.assert_called_once_with(
        "SELECT id, email, first_name, last_name, phone_cc, phone, company, company_vat, company_number, city, state, postcode, country, currency, address_1 FROM client WHERE timestamp = %s", 
        (uuid,)
    )

@patch('invoice_kassa_consumer.mysql.connector.connect')
def test_get_client_by_uuid_not_found(mock_connect):
    """Test client lookup when UUID doesn't exist"""
    mock_cursor = MagicMock()
    mock_connect.return_value.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None
    
    uuid = 'nonexistent-uuid'
    result = get_client_by_uuid(uuid)
    
    assert result is None

@patch('invoice_kassa_consumer.get_client_by_uuid')
@patch('invoice_kassa_consumer.mysql.connector.connect')
def test_create_invoice_success(mock_connect, mock_get_client, sample_invoice_data, sample_client_info):
    """Test successful invoice creation"""
    # Setup mocks
    mock_get_client.return_value = sample_client_info
    mock_cursor = MagicMock()
    mock_connect.return_value.cursor.return_value = mock_cursor
    mock_cursor.lastrowid = 42  # Simulate inserted invoice ID
    
    result = create_invoice(sample_invoice_data)
    
    assert result is True
    mock_cursor.execute.assert_called()  # Should be called for both invoice and items
    mock_connect.return_value.commit.assert_called_once()

@patch('invoice_kassa_consumer.get_client_by_uuid')
def test_create_invoice_client_not_found(mock_get_client, sample_invoice_data):
    """Test invoice creation when client not found"""
    mock_get_client.return_value = None
    
    with pytest.raises(ValueError, match="Client not found for UUID:"):
        create_invoice(sample_invoice_data)

@patch('invoice_kassa_consumer.parse_invoice_xml')
@patch('invoice_kassa_consumer.create_invoice')
def test_on_message_success(mock_create_invoice, mock_parse, sample_invoice_data):
    """Test successful message processing"""
    # Setup mocks
    mock_parse.return_value = sample_invoice_data
    mock_create_invoice.return_value = True
    
    mock_channel = MagicMock()
    mock_method = MagicMock()
    mock_properties = MagicMock()
    
    body = b"<test>xml</test>"
    on_message(mock_channel, mock_method, mock_properties, body)
    
    mock_parse.assert_called_once_with(body.decode())
    mock_create_invoice.assert_called_once_with(sample_invoice_data)
    mock_channel.basic_ack.assert_called_once_with(mock_method.delivery_tag)

@patch('invoice_kassa_consumer.parse_invoice_xml')
def test_on_message_parse_error(mock_parse):
    """Test message processing when XML parsing fails"""
    mock_parse.side_effect = Exception("Parse error")
    
    mock_channel = MagicMock()
    mock_method = MagicMock()
    mock_properties = MagicMock()
    
    body = b"<test>xml</test>"
    on_message(mock_channel, mock_method, mock_properties, body)
    
    mock_channel.basic_nack.assert_called_once_with(mock_method.delivery_tag, requeue=False)

@patch('invoice_kassa_consumer.pika.BlockingConnection')
def test_start_consumer(mock_connection):
    """Test consumer startup"""
    mock_channel = MagicMock()
    mock_connection.return_value.channel.return_value = mock_channel
    
    # Simulate keyboard interrupt to stop the test
    mock_channel.start_consuming.side_effect = KeyboardInterrupt()
    
    # Set environment variables needed by the function
    with patch.dict(os.environ, {
        'RABBITMQ_HOST': 'localhost',
        'RABBITMQ_PORT': '5672',
        'RABBITMQ_USER': 'guest',
        'RABBITMQ_PASSWORD': 'guest'
    }):
        start_consumer()
    
    mock_channel.queue_declare.assert_called()
    mock_channel.basic_consume.assert_called()
    mock_channel.stop_consuming.assert_called_once()
    mock_connection.return_value.close.assert_called_once()