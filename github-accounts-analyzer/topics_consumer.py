from confluent_kafka import Consumer, KafkaError, KafkaException
from loguru import logger

from config import consumer_conf


class TopicsConsumer():

    def __init__(self, conf: dict, topics: str, group_id: str):
        self.consumer = Consumer(**{**conf, 'group.id': group_id})
        self.consumer.subscribe(topics)
        # self.topics = topics

    # def consume(self):
    #     partition_eof = False
    #     try:
    #         self.consumer.subscribe(self.topics)

    #         while not partition_eof:
    #             msg = self.consumer.poll(timeout=1.0)
    #             if msg is None:
    #                 continue

    #             if msg.error():
    #                 if msg.error().code() == KafkaError._PARTITION_EOF:
    #                     # End of partition event
    #                     logger.info('%% %s [%d] reached end at offset %d\n' %
    #                                 (msg.topic(), msg.partition(), msg.offset()))
    #                     # partition_eof = True
    #                 elif msg.error():
    #                     raise KafkaException(msg.error())
    #             else:
    #                 logger.debug(f'Consume key:{msg.key()} msg:{msg.value()}')
    #     finally:
    #         # Close down consumer to commit final offsets.
    #         logger.debug('Close consumer')
    #         self.consumer.close()

    def get_msg(self):
        msg = self.consumer.poll(timeout=1.0)
        # logger.debug(f'Consume key:{msg.key()} msg:{msg.value()}')
        return msg
            
    def close(self):
        self.consumer.close()


if __name__ == '__main__':
    p = TopicsConsumer(consumer_conf, topics=['test'], group_id='test_app')
    p.consume()
