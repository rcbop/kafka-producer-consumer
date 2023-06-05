""" Main module for producer application. """
from kink import inject

from producer.bootstrap import bootstrap
from producer.handler import KafkaTopicProducer


@inject
def main(producer: KafkaTopicProducer):
    producer.start()


if __name__ == "__main__":
    bootstrap()
    main()
