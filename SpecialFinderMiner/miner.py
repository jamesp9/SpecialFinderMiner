#!/usr/bin/env python
from sqlalchemy.sql import func
from models.tables import DataAccessLayer, Item
from elasticsearch import Elasticsearch, TransportError
from utils import config_logger
from config import config
import logging

dal = DataAccessLayer()
logger = logging.getLogger(__file__)

class LowestPriceFinder(object):

    lowest_price_mapping = {
        'properties': {
            'title': {
                'type': 'string'
            },
            'price': {
                'type': 'double'
            },
            'per': {
                'type': 'string'
            },
            'url': {
                'type': 'string'
            },
            'vendor': {
                'type': 'string'
            }
        }
    }

    @staticmethod
    def lowest_price_doc(title, price, per, url, vendor):
        lowest_price_doc = {
            'title': title,
            'price': price,
            'per': per,
            'url': url,
            'vendor': vendor
        }
        return lowest_price_doc

    @staticmethod
    def lowest_price_query(title, per, vendor):
        lowest_price_query = \
        {'query':
             {
               'bool': {
                  'must': [
                    {
                        'term': {
                            'title': title
                        }
                    },
                    {
                        'term': {
                            'per': per
                        }
                    },
                    {
                        'term': {
                            'vendor': vendor
                        }
                    }
                   ]
                }
             }
        }
        return lowest_price_query

    es_type = 'lowest_price'

    def __init__(self):
        self.lowest_price_dict = {}
        try:
            self.index_name = config.elasticsearch.index
        except AttributeError as e:
            logger.fatal(u'Incomplete configuration')
            raise SystemExit(-1)

    def create_index_mapping(self):
        try:
            # Even fine if the index is existed.
            es.indices.create('specialfinder', ignore=[400])
            es.indices.put_mapping(doc_type=self.es_type,
                                   body=self.lowest_price_mapping,
                                   index='specialfinder')
        except TransportError as e:
            logger.error(u'Failed to create index or mapping: %s', e)

    def update_lowest_price(self):
        self.create_index_mapping()
        lowest_prices = dal.session.query(Item.title,
                                          Item.per,
                                          Item.vendor,
                                          Item.url,
                                          func.min(Item.price).label('min_price'),
                                         ).group_by(Item.title,
                                                    Item.per,
                                                    Item.vendor,
                                                    Item.url).all()
        lowest_prices_dict = {(title, per, vendor): (price, url)
                              for title, per, vendor, url, price in
                              lowest_prices}
        for t, p in lowest_prices_dict.items():
            # FIXME: error handling here
            res = es.search(index=self.index_name,
                            doc_type=self.es_type,
                            body=self.lowest_price_query(t[0], t[1], t[2]))

            if not res['hits']['hits']:  # The item is not existed
                logger.info(u'Lower price of %s found at the first time: %f',
                             t[0], p[0])
                es.create(index=self.index_name,
                          doc_type=self.es_type,
                          body=self.lowest_price_doc(t[0], p[0], t[1],
                                                     p[1], t[2]))
            else:
                print "aaa"


def init():
    global es
    try:
        dal.conn_str = config.db_conn
        dal.connect()
        es = Elasticsearch(config.elasticsearch.hosts)
        config_logger(logger)
    except AttributeError as e:
        logger.fatal(u'Incomplete configuration')
        raise SystemExit(-1)
    except Exception as e: # TODO: too broad
        logger.fatal(u'Failed to connect to the db')
        raise SystemExit(-1)

def main():
    init()
    LowestPriceFinder().update_lowest_price()


if __name__ == '__main__':
    main()
