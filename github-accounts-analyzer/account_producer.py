import json

from config import logger, producer_conf
from topic_producer import TopicProducer

TOPIC = 'test'
PATH = 'data/account_data.txt'


class AccountProducer():

    def __init__(self, conf: dict, topic: str):
        self.producer = TopicProducer(conf, topic)

    def load_file(self, path: str):
        with open(path) as file:
            data = file.readlines()
            data = [json.loads(d) for d in data]
        logger.debug(data)
        return data

    def produce(self, path):
        data = self.load_file(path)
        for account in data:
            login = account.get('login')
            interval = account.get('interval')
            if login and interval:
                msg = json.dumps({'login': login,
                                  'interval': interval})
                self.producer.produce(key=login,
                                      msg=msg)


if __name__ == '__main__':
    logger.debug(producer_conf)
    p = AccountProducer(producer_conf, topic=TOPIC)
    # p.load_file(PATH)
    p.produce(PATH)
