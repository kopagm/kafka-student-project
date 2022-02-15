import json
import re
from datetime import datetime, timedelta

from confluent_kafka import Consumer, KafkaError, KafkaException
# from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

from config import consumer_conf, github_api, logger, producer_conf, commit_model
from githubapi.github_service import GitHubService
from topic_producer import TopicProducer
from topics_consumer import TopicsConsumer


class AccountAnalyzer():

    def __init__(self, consumer: TopicsConsumer, producer: TopicProducer, github_service: GitHubService):
        self.consumer = consumer
        self.producer = producer
        self.github_service = github_service

    def run(self):

        while True:
            msg = self.get_account_msg()
            logger.debug(msg)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # deser key, val
                account = self.deserialize_account(msg)
                logger.debug(account)
                # w2 -> from_date
                if account:
                    starting_datetime = self.count_starting_datetime(
                        account.get('interval'))
                    logger.debug(starting_datetime)
                    if starting_datetime:
                        # get commit info
                        commits_info = self.get_commit_info(
                            account.get('login'), starting_datetime)
                        logger.debug(commits_info)
                        # produce to topic
                        self.send_commit_msgs(commits_info)

    def deserialize_account(self, msg):
        if msg.value() is not None:
            logger.debug(msg.value())
            try:
                account = json.loads(msg.value())
            except json.JSONDecodeError:
                account = None
        else:
            account = None
        return account

    def count_starting_datetime(self, interval: str):
        starting_datetime = None
        if isinstance(interval, str):
            match = re.match('^(\d+)([hdw])$', interval)
            if match:
                (n, period) = match.groups()
                if period == 'w':
                    starting_datetime = datetime.now() - timedelta(weeks=int(n))
                elif period == 'd':
                    starting_datetime = datetime.now() - timedelta(days=int(n))
                elif period == 'h':
                    starting_datetime = datetime.now() - timedelta(hours=int(n))
        if starting_datetime:
            starting_datetime = starting_datetime.isoformat()   
        return starting_datetime

    def get_commit_info(self, login: str, starting_datetime: datetime) -> list:
        if login and starting_datetime:
            return self.github_service.pollCommits(
                login=login, starting_datetime=starting_datetime)

    def get_account_msg(self):
        return self.consumer.get_msg()

    def send_commit_msgs(self, commits_info: list):
        logger.debug(commits_info)
        for commit_info in commits_info:
            key = commit_info.get('url')
            # logger.debug(key)
            msg = json.dumps(commit_info)
            self.producer.produce(msg, key)


if __name__ == '__main__':
    try:
        # github_api = GitHubApi(github_token)
        # github_api = Github(github_token)
        github_service = GitHubService(github_api, commit_model)
        topic_consumer = TopicsConsumer(
            consumer_conf, topics=['test'], group_id='test_app')
        topic_producer = TopicProducer(producer_conf, topic='test_commits')
        app = AccountAnalyzer(consumer=topic_consumer,
                              producer=topic_producer, github_service=github_service)
        app.run()
    finally:
        topic_consumer.close()
        topic_producer.flush()
