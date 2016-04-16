from attrdict import AttrDict
import yaml
import os

DEFAULT_CONF = AttrDict({
    'log_level': 'DEBUG',
    'queue': {
        'conn': 'amqp://guest:guest@localhost//',
        'exchange': {'name': 'scrapy_exchange', 'type': 'fanout'},
        'queue': {'name': 'scrapy_result_db'},
        'serializer': ['msgpack'],
    },
    'db_conn': 'postgresql://dev:dev@localhost:5434/dev',

})

def load_config(conf_path=None):
    if conf_path:
        with open(conf_path=conf_path) as fp:
            conf = AttrDict(yaml.safe_load(fp))
            return conf
    # No conf path was defined, default one will be used.
    return DEFAULT_CONF


conf_path = os.environ.get('SPECIAL_FINDER_MINER')
config = load_config(conf_path)
