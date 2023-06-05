from confluent_kafka import Consumer, KafkaException
from kink import inject

@inject
class KafkaTopicConsumer:
    def __init__(self, consumer_config: dict, topic: str):
        self.consumer = Consumer(consumer_config)
        self.topic = topic

    def start(self, continue_running=lambda: True):
        print(f"Subscribing to topic: {self.topic}")
        self.consumer.subscribe([self.topic])

        while True and continue_running():
            try:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    continue

                print('Received message: {}'.format(
                    msg.value().decode('utf-8')))
                self.consumer.commit(msg)
            except KafkaException as ex:
                print('Exception in consumer: {}'.format(str(ex)))
                continue

        self.consumer.close()
