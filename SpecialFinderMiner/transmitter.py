#!/usr/bin/env python
from elasticsearch import Elasticsearch, TransportError, RequestError
from sqlalchemy import exc
from models.tables import DataAccessLayer, Item
from datetime import date, timedelta
from utils import config_logger
from config import config
import logging
import argparse

class Transmitter(object):

    es_type = 'specialfinder_items'

    items_mapping = {
        'properties': {
            'title': {
                'type': 'string',
            },
            'url': {
                'type': 'string',
                'index': 'not_analyzed',
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
            },
            'date': {
                'type': 'date',
                'index': 'not_analyzed'
            }
        }
    }

    def __init__(self):
        try:
            self.logger = logging.getLogger(type(self).__name__)
            config_logger(self.logger)
            self.dal = DataAccessLayer()
            self.dal.conn_str = config.db_conn

            # Configure elasticsearch
            self.es = Elasticsearch(config.elasticsearch.hosts)
            self.index_name = config.elasticsearch.index.special_items
            self.dal.connect()
        except AttributeError as e:
            logger.fatal(u'Incomplete configuration: %s', e)
            raise SystemExit(-1)
        except (exc.DBAPIError,exc.InvalidRequestError) as e:
            self.logger.fatal(u'Failed to connect to the db')
            raise SystemError(-1)

    def create_index_mapping(self):
        """
        Create index and mapping in the elasticsearch
        """
        try:
            # Even fine if the index is existed.
            self.es.indices.create('specialfinder', ignore=[400])
            self.es.indices.put_mapping(doc_type=self.es_type,
                                        body=self.items_mapping,
                                        index=self.index_name)
        except TransportError as e:
            self.logger.error(u'Failed to create index or mapping: %s', e)
            return False
        return True

    @staticmethod
    def item_doc(title, url, price, per, vendor, date):
        """
        Return an item document to insert to the elasticsearch
        @return: items_doc
        """
        item_doc = {
            'title': title,
            'url': url,
            'price': price,
            'per': per,
            'vendor': vendor,
            'date': date
        }
        return item_doc

    def transmit(self, days=1):
        """
        Transmit data from db to elasticsearch
        """
        try:
            items = self.dal.session.query(Item.title,
                                           Item.url,
                                           Item.price,
                                           Item.per,
                                           Item.vendor,
                                           Item.date)
            if days != -1:
                date_of_data = date.today() - timedelta(days)
                self.logger.info(u'Going to import data of %s', date_of_data)
                items = items.filter(Item.date==date_of_data)

            counter = 0
            for item in items:
                self.logger.debug(u'Add item: %s', item)
                self.es.create(index=self.index_name,
                               doc_type=self.es_type,
                               body=self.item_doc(item[0],
                                                  item[1],
                                                  item[2],
                                                  item[3],
                                                  item[4],
                                                  item[5]))
                counter += 1
            self.logger.info(u'Add %d items', counter)
        except TransportError as e:
            self.logger.error(u'Failed to transmit data: %s', e)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days',
                        help='Transfer x days of data, -1 means all data',
                        type=int,
                        default=1)
    args = parser.parse_args()

    t = Transmitter()
    t.transmit(args.days)

if __name__ == '__main__':
    main()
