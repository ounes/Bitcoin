import logging
import elastic_config

config = {
    'elasticsearch': {
        'hosts': ['db'],
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
        'topic_name': 'transaction_historical'
    }
}

logging.basicConfig(**config['logger'])
