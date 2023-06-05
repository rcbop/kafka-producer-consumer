"""This is the main entry point for the consumer application."""
from kink import inject

from consumer.bootstrap import bootstrap
from consumer.handler import KafkaTopicConsumer


@inject
def main(consumer: KafkaTopicConsumer):
    consumer.start()


if __name__ == "__main__":
    bootstrap()
    main()
