from sqlalchemy import (Column, Integer, String, Unicode,
                        Float, Date, create_engine, UniqueConstraint)
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base

class DataAccessLayer(object):

    def __init__(self):
        self.engine = None
        self.conn_str = None

    def connect(self):
        self.engine = create_engine(self.conn_str)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.engine.connect()

class Base(object):
    id = Column(Integer, primary_key=True)

Base = declarative_base(cls=Base)

class Item(Base):
    __tablename__ = 'items'
    __table_args__ = (UniqueConstraint('title', 'date'), )

    title = Column(Unicode(255), nullable=False, index=True)
    price = Column(Float, nullable=False)
    per = Column(String(25), nullable=True)
    url = Column(String(255), nullable=True)
    image_url = Column(String(255), nullable=True)
    date = Column(Date, nullable=False)
    vendor = Column(Unicode(50), nullable=False)

class LowestPriceItem(Base):
    __tablename__ = 'min_items'

    title = Column(Unicode(255), nullable=False, index=True)
    price = Column(Float, nullable=False)
    per = Column(String(25), nullable=True)
    url = Column(String(255), nullable=True)
    image_url = Column(String(255), nullable=True)
    vendor = Column(Unicode(50), nullable=False)
