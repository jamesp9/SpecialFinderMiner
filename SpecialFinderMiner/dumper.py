#!/usr/bin/env python
from kombu.mixins import ConsumerMixin
from kombu import Connection, Exchange, Queue
# FIXME resolve import issues.
from config import config
from utils import config_logger
from models.tables import DataAccessLayer, Item
from sqlalchemy import exc
import logging
import sys

class Dumper(ConsumerMixin):

    def __init__(self, connection=None):
        self.logger = logging.getLogger(type(self).__name__)
        config_logger(self.logger)

        queue_config = config.queue
        if connection:
            self.connection = self.connection
        else:
            self.connection = Connection(queue_config.conn)

        self.exchange = Exchange(name=queue_config.exchange.name,
                                 type=queue_config.exchange.type)
        self.queue = Queue(queue_config.queue.name,
                           exchange=self.exchange,
                           channel=self.connection)
        self.dal = DataAccessLayer()
        self.dal.conn_str = config.db_conn

        try:
            self.connection.connect()
            self.dal.connect()
        except Exception,e:
            self.logger.fatal(u'Failed to connect to the db or queue')
            raise SystemError(-1)

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        self.logger.info(u'Dumper is ready for receiving result')
        super(Dumper, self).on_consume_ready(connection,
                                             channel,
                                             consumers,
                                             **kwargs)

    def on_decode_error(self, message, exc):
        self.logger.error(exc)
        super(Dumper, self).on_decode_error(message, exc)

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(queues=[self.queue],
                     callbacks=[self.dump_result],
                     accept=config.queue.serializer
                    )
        ]

    @staticmethod
    def _extract_item(item):
        '''
        Scrapy puts item(s) in the list, so need extract it from the list.
        '''
        if isinstance(item, list) and len(item):
            return item[0]
        else:
            return item

    def dump_result(self, body, message):
        # TODO: check json schema
        self.logger.debug(u'Received: %s', body)
        try:
            title = self._extract_item(body['title'])
            price = self._extract_item(body['price'])
            per = self._extract_item(body.get('per'))
            url = self._extract_item(body['url'])
            image_url = self._extract_item(body['image_url'])
            date = self._extract_item(body['date'])
            vendor = self._extract_item(body['vendor'])
            item = Item(title=title,
                        price=price,
                        per=per,
                        url=url,
                        image_url=image_url,
                        date=date,
                        vendor=vendor)
            self.dal.session.add(item)
            self.dal.session.commit()
        except (exc.DBAPIError,exc.InvalidRequestError), e:
            self.logger.error(u'DB error occurred: %s', e)
            if e.connection_invalidated:
                self.logger.info(u'Try re-connecting to the db')
                self.dal.connect()
                message.reject(requeue=True)
            else:
                self.dal.session.rollback()
                message.reject() # Probably invalid result, discard
        except KeyError, e:
            self.logger.error(u'Invalid result: %s', e)
            message.ack()  # Remove invalid message
        except Exception, e:
            self.logger.error(u'Error occurred when dumping the result: %s', e)
            message.reject(requeue=True)  # Requeue the message
        else:
            message.ack()


def main():
    reload(sys)
    sys.setdefaultencoding('utf8')
    Dumper().run()

if __name__ == '__main__':
    main()
