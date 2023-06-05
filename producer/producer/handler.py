from random import choice

from confluent_kafka import Producer
from kink import inject

msgs_to_send = [
    "Hello my friend, we meet again! It's been a while since you've been here.",
    "Feels like forever, within my heart are memories of perfect love that you gave to me.",
    "Oh, I remember, when you are with me, I'm free, I'm careless, I believe.",
    "Above all the others, we'll fly, this brings tears to my eyes, my sacrifice.",
    "It's the eye of the tiger, it's the thrill of the fight, rising up to the challenge of our rival.",
    "And the last known survivor stalks his prey in the night, and he's watching us all with the eye of the tiger.",
    "Face to face, out in the heat, hanging tough, staying hungry.",
    "They stack the odds 'til we take to the street, for we kill with the skill to survive.",
]

@inject
class KafkaTopicProducer:
    def __init__(self, producer_config: dict, topic: str):
        self.producer = Producer(producer_config)
        self.topic = topic

    def start(self, continue_running=lambda: True):
        print(f"Producing to topic: {self.topic}")
        while True and continue_running():
            msg = choice(msgs_to_send)
            print(f"Sending message: {msg}")
            self.producer.produce(self.topic, msg.encode('utf-8'))
            self.producer.flush()

        self.producer.close()
