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
    },
    'kafka': {
        'host': 'kafka',
        'port':  '9092'
    },
    'logger': {
        'level': logging.INFO
    },
    'price_streaming': {
        'host': 'price_streaming_producer',
        'port': 9092
    }
}

logging.basicConfig(**config['logger'])
