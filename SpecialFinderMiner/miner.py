#!/usr/bin/env python
from sqlalchemy.sql import func
from sqlalchemy import exc
from elasticsearch import Elasticsearch, TransportError, RequestError
from datetime import date, timedelta

from models.tables import DataAccessLayer, Item
from notifier import Notifier
from utils import config_logger
from config import config
import logging

class SpecialFinder(object):

    es_type = 'specialfinder_items'

    @staticmethod
    def special_query(title, operator="and", from_date='1970-01-01'):
        """
        Return a query to find special of an item in the elasticsearch
        @return: item query
        """
        special_query = \
        {"query": {
             "filtered": {
                 "query": {
                     "match": {
                         "title": {
                             "query": title,
                             "operator": operator
                         }
                     }
                 },
                 "filter": {
                     "range": {
                         "date": {
                             "from": from_date
                         }
                     }
                 }
             }

        }}
        return special_query

    def find_special(self, titles=None):

        if titles is None:
            try:
                titles = config.miner.special_titles
            except AttributeError as e:
                logger.error(u'Failed to find titles in the config: %s', e)
                return

        from_date = date.today() - timedelta(days=7)  # FIXME: better way to define from_date

        for title_entry in titles:
            try:
                title = title_entry.title
                operator = title_entry.get('operator', 'and')
                res = es.search(index=specialfinder_index,
                                doc_type=self.es_type,
                                body=self.special_query(title,
                                                        operator,
                                                        from_date))

                num_special_found = res['hits']['total']
                logger.info(u'Found %d specials for %s from %s',
                            num_special_found, title, from_date)
                if num_special_found > 0:

                    for special in res['hits']['hits']:
                        source = special['_source']
                        logger.debug('Special: %s', source)
                        msg = "{title} is on special: {price}, {url}".format(
                            title=source['title'],
                            price=source['price'],
                            url=source['url']
                        )
                        notifier.send_message(msg)

            except AttributeError as e:
                logger.error(u'Failed to get the title: %s', e)
            except KeyError as e:
                logger.error(u'Invalid response: %s', e)


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
        logger.debug(u'Query: %s', lowest_price_query)
        return lowest_price_query


    def create_index_mapping(self):
        """
        Create index and mapping in the elasticsearch
        """
        try:
            # Even fine if the index is existed.
            es.indices.create(lowest_price_index, ignore=[400])
            es.indices.put_mapping(doc_type=self.es_type,
                                   body=self.lowest_price_mapping,
                                   index=lowest_price_index)
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
            try:
                logger.debug(u'Search entry %s', t)
                res = es.search(index=lowest_price_index,
                                doc_type=self.es_type,
                                body=self.lowest_price_query(t[0], t[1], t[2]))

                if not res['hits']['hits']:  # The item is not existed
                    logger.info(u'Lower price of %s found at the first time: %f',
                                t[0], p)
                    es.create(index=lowest_price_index,
                              doc_type=self.es_type,
                              body=self.lowest_price_doc(t[0], p, t[1], t[2]))
                else:
                    item = res['hits']['hits'][0]
                    price = float(item['_source']['price'])
                    title = item['_source']['title']
                    if p < price: # If lower price of the item is found
                        msg = u'Lower price of "{item}" found at {vendor}: {price}'.format(
                            item=t[0], price=p, vendor=t[2])
                        logger.info(msg)
                        notifier.send_message(msg)  # Send notification
                        es.update(index=lowest_price_index,
                                  doc_type=self.es_type,
                                  id=item['_id'],
                                  body={"doc": {"price": p}})
                        logger.info(u'Updated item %s with price %f',
                                 item['_id'], p)
            except TransportError as e:
                logger.info(u'Error occurred when updating lowest price: %s',
                            e)
def init():
    global es, dal, logger, notifier, specialfinder_index, lowest_price_index
    try:
        logger = logging.getLogger(__file__)

        # Configure db
        dal = DataAccessLayer()
        config_logger(logger)
        dal.conn_str = config.db_conn
        dal.connect()

        # Configure elasticsearch
        es = Elasticsearch(config.elasticsearch.hosts)
        specialfinder_index = config.elasticsearch.index.special_items
        lowest_price_index = config.elasticsearch.index.lowest_price
        # Notifier
        notifier = Notifier()
    except AttributeError as e:
        logger.fatal(u'Incomplete configuration: %s', e)
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
    SpecialFinder().find_special()

if __name__ == '__main__':
    main()
