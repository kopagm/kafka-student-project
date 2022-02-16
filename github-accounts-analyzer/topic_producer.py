from confluent_kafka import Producer, Message
from loguru import logger

from config import producer_conf


class TopicProducer():

    def __init__(self, conf: dict, topic: str):
        # self.conf = conf
        self.producer = Producer(**conf)
        self.topic = topic

    def delivery_callback(self, err, msg):
        if err:
            logger.info('%% Message failed delivery: %s\n' % err)
        else:
            logger.info('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    def produce(self, msg, key=None):
        self.producer.produce(self.topic, msg, key, callback=self.delivery_callback)
        self.producer.poll(1)

    def flush(self):
        self.producer.flush()

# if __name__ == '__main__':
    # p = TopicProducer(producer_conf, topic='test')
    # p.produce('test1')
    # p.produce({'a': 'a'})