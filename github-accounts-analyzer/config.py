import github
from github import Github
from loguru import logger
from githubapi.commit_model import CommitModel
from github_token import github_token

logger.add('app.log', rotation='0.2 MB',
           retention=1, enqueue=True, diagnose=True)

server = 'localhost'
# server = '192.168.1.4'
# brokers = 'localhost:9092,localhost:9093'
brokers = f'{server}:9092'

# Producer configuration
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
producer_conf = {'bootstrap.servers': brokers}

consumer_conf = {'bootstrap.servers': brokers,
                 'group.id': 'logins',
                #  'auto.offset.reset': 'beginning',
                 'auto.offset.reset': 'earliest',
                #  'enable.partition.eof': 'true',
                 }

github_api = Github(github_token)
commit_model = CommitModel

