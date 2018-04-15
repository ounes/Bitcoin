import logging
import elastic_config

config = {
    'elasticsearch': {
        'hosts': ['db'], # TODO put it in elasticsearch_config
        'username': 'elastic',
        'password': elastic_config.password
    },
    'zookeeper': {
        'host': 'zookeeper',
        'port': '2181'
    },
    'kafka': {
        'host': 'kafka',
        'port':  '9092'
    },
    'logger': {
        'level': logging.INFO
    },
    'spark_job': {
        'master': 'local[2]',
        'app_name': 'Historical Transaction',
    },
    'topic_config': {
        'group_id': 'transaction',
        'topic_name': 'transaction_historical' # TODO auto gen on build!
    }
}

logging.basicConfig(**config['logger'])
