import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
import heartbeat


@pytest.fixture
def mock_env_vars():
    with patch.dict("os.environ", {
        "RABBITMQ_HOST": "localhost",
        "RABBITMQ_PORT": "5672",
        "RABBITMQ_USER": "guest",
        "RABBITMQ_PASSWORD": "guest"
    }):
        yield


def test_get_rabbitmq_connection_success(mock_env_vars):
    with patch("pika.BlockingConnection") as mock_connection:
        connection = heartbeat.get_rabbitmq_connection()
        assert connection is not None
        mock_connection.assert_called_once()


def test_get_rabbitmq_connection_failure(mock_env_vars):
    with patch("pika.BlockingConnection", side_effect=Exception("Connection failed")):
        connection = heartbeat.get_rabbitmq_connection()
        assert connection is None


def test_dict_to_xml_format():
    test_data = {
        "ServiceName": "TestService",
        "Status": "Online",
        "Timestamp": "2025-05-24T12:00:00.000Z",
        "HeartBeatInterval": "60",
        "Version": "1.0",
        "Host": "localhost",
        "Environment": "Development"
    }

    xml_output = heartbeat.dict_to_xml(test_data)
    assert "<ServiceName>TestService</ServiceName>" in xml_output
    assert "<Status>Online</Status>" in xml_output
    assert "<Timestamp>2025-05-24T12:00:00.000Z</Timestamp>" in xml_output


def test_create_heartbeat_message(mock_env_vars):
    xml_message = heartbeat.create_heartbeat_message()
    assert "<Heartbeat>" in xml_message
    assert "<ServiceName>Facturatie</ServiceName>" in xml_message
    assert "<Status>Online</Status>" in xml_message
    assert "<Host>localhost</Host>" in xml_message


def test_send_heartbeat_success(mock_env_vars):
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    with patch("heartbeat.get_rabbitmq_connection", return_value=mock_connection):
        heartbeat.send_heartbeat()

        mock_channel.queue_declare.assert_called_once_with(queue="controlroom_heartbeat", durable=True)
        mock_channel.basic_publish.assert_called_once()
        mock_connection.close.assert_called_once()


def test_send_heartbeat_failure(mock_env_vars):
    with patch("heartbeat.get_rabbitmq_connection", return_value=None):
        with patch.object(heartbeat.logger, "error") as mock_log_error:
            heartbeat.send_heartbeat()
            mock_log_error.assert_called_with("Kan geen verbinding maken met RabbitMQ!")
