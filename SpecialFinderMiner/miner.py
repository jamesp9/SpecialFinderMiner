#!/usr/bin/env python
from sqlalchemy.sql import func
from models.tables import DataAccessLayer, Item, LowestPriceItem
from config import config
import logging

dal = DataAccessLayer()
logger = logging.getLogger(__file__)

class LowestPriceFinder(object):

    def __init__(self):
        self.lowest_price_dict = {}

    def update_lowest_price(self):
        lowest_prices = dal.session.query(Item.title,
                                          Item.per,
                                          func.max(Item.price).label('min_price'),
                                         ).group_by(Item.title, Item.per).all()
        lowest_prices_dict = {(title,per): price
                              for title, per, price in lowest_prices}
        print lowest_prices_dict

def config_db():
    dal.conn_str = config.db_conn
    try:
        dal.connect()
    except Exception as e: # TODO: too broad
        logger.fatal(u'Failed to connect to the db')
        raise SystemExit(-1)

def main():
    config_db()
    LowestPriceFinder().update_lowest_price()


if __name__ == '__main__':
    main()
