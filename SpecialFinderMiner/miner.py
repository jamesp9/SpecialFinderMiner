#!/usr/bin/env python
from sqlalchemy.sql import func
from sqlalchemy import exc
from models.tables import DataAccessLayer, Item
from elasticsearch import Elasticsearch, TransportError, RequestError
from utils import config_logger
from config import config
import logging

dal = DataAccessLayer()
logger = logging.getLogger(__file__)

class LowestPriceFinder(object):

    lowest_price_mapping = {
        'properties': {
            'title': {
                'type': 'string',
                'index' : 'not_analyzed'
            },
            'price': {
                'type': 'double',
                'index' : 'not_analyzed'
            },
            'per': {
                'type': 'string',
                'index' : 'not_analyzed'
            },
            'vendor': {
                'type': 'string',
                'index' : 'not_analyzed'
            }
        }
    }

    es_type = 'lowest_price'

    @staticmethod
    def lowest_price_doc(title, price, per, vendor):
        """
        Return a lowest price document to insert to the elasticsearch
        @return: lowest price document
        """
        lowest_price_doc = {
            'title': title,
            'price': price,
            'per': per,
            'vendor': vendor
        }
        return lowest_price_doc

    @staticmethod
    def lowest_price_query(title, per, vendor):
        """
        Return a query to find the lowest price of an item in the elasticsearch
        @return: lowest price query
        """
        lowest_price_query = \
        {"query":
             {
               "bool": {
                  "must": [
                    { "term": {"title": title} },
                    { "term": {"per": per} },
                    { "term": {"vendor": vendor} }
                   ]
                }
             }
        }
        return lowest_price_query

    def __init__(self):
        try:
            self.index_name = config.elasticsearch.index
        except AttributeError as e:
            logger.fatal(u'Incomplete configuration')
            raise SystemExit(-1)

    def create_index_mapping(self):
        """
        Create index and mapping in the elasticsearch
        """
        try:
            # Even fine if the index is existed.
            es.indices.create('specialfinder', ignore=[400])
            es.indices.put_mapping(doc_type=self.es_type,
                                   body=self.lowest_price_mapping,
                                   index='specialfinder')
        except TransportError as e:
            logger.error(u'Failed to create index or mapping: %s', e)
            return False
        return True

    def update_lowest_price(self):
        """
        Get the lowest prices from database and compare with the ones
        recorded in the Elasticsearch. If found lower prices, then update.
        """

        res = self.create_index_mapping()
        if not res:
            logger.error(u'Failed to update lowest prices')
            return

        try:
            lowest_prices = dal.session.query(Item.title,
                                              Item.per,
                                              Item.vendor,
                                              func.min(Item.price),
                                             ).group_by(Item.title,
                                                        Item.per,
                                                        Item.vendor).all()
        except (exc.DBAPIError,exc.InvalidRequestError) as e:
            logger.error(u'Failed to get lowest prices from db: %s', e)
            return

        lowest_prices_dict = {(title, per, vendor): float(price)
                              for title, per, vendor, price in
                              lowest_prices}


        for t, p in lowest_prices_dict.items():
            # FIXME: error handling here
            try:
                res = es.search(index=self.index_name,
                                doc_type=self.es_type,
                                body=self.lowest_price_query(t[0], t[1], t[2]))

                if not res['hits']['hits']:  # The item is not existed
                    logger.info(u'Lower price of %s found at the first time: %f',
                                t[0], p)
                    es.create(index=self.index_name,
                              doc_type=self.es_type,
                              body=self.lowest_price_doc(t[0], p, t[1], t[2]))
                else:
                    item = res['hits']['hits'][0]
                    price = float(item['_source']['price'])
                    title = item['_source']['title']
                    if p < price: # If lower price of the item is found
                        logger.info(u'Lower price of %s found: %f', t[0], p)
                        es.update(index=self.index_name,
                                  doc_type=self.es_type,
                                  id=item['_id'],
                                  body={"doc": {"price": p}})
                        logger.info(u'Updated item %s with price %f',
                                 item['_id'], p)
            except TransportError as e:
                logger.info(u'Error occurred when updating lowest price: %s',
                            e)
def init():
    global es
    try:
        config_logger(logger)
        dal.conn_str = config.db_conn
        dal.connect()
        es = Elasticsearch(config.elasticsearch.hosts)
    except AttributeError as e:
        logger.fatal(u'Incomplete configuration')
        raise SystemExit(-1)
    except (exc.DBAPIError,exc.InvalidRequestError) as e:
        logger.fatal(u'Failed to connect to the db: %s', e)
        raise SystemExit(-1)
    except Exception as e:
        logger.fatal(u'Failed to connect to the db')
        raise SystemExit(-1)

def main():
    init()
    LowestPriceFinder().update_lowest_price()

if __name__ == '__main__':
    main()
