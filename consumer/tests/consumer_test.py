from unittest.mock import MagicMock, Mock, patch

import pytest

from consumer.consumer import KafkaTopicConsumer

TOPIC = "your_kafka_topic"
MESSAGE = b"Test message"


@pytest.mark.unit
def test_consume_message():
    mock_consumer = MagicMock()
    mock_msg = Mock()
    mock_consumer.poll.return_value = mock_msg
    mock_msg.error.return_value = None
    mock_msg.value.return_value = b'Test message'

    mock_consumer.poll.return_value = mock_msg
    mock_consumer.subscribe.return_value = None
    mock_consumer.commit.return_value = None

    config = {
        "bootstrap.servers": "test_kafka_bootstrap_servers",
        "group.id": "test_consumer_group_id",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "security.protocol": "ssl",
        "ssl.cafile": "/path/to/ca.crt",
        "ssl.certfile": "/path/to/client.crt",
        "ssl.keyfile": "/path/to/client.key",
        "topic": TOPIC
    }

    with patch('consumer.consumer.Consumer', return_value=mock_consumer):
        consumer = KafkaTopicConsumer(config, TOPIC)
        consumer.start(Mock(side_effect=[True, False]))

        # Assertions
        mock_consumer.subscribe.assert_called_once_with([TOPIC])
        mock_consumer.poll.assert_called_once_with(1.0)
        mock_msg.error.assert_called_once()
        mock_msg.value.assert_called_once()
        mock_consumer.commit.assert_called_once_with(mock_msg)
        assert mock_msg.value() == MESSAGE
        mock_consumer.close.assert_called_once()
