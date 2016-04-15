from sqlalchemy import (Column, Integer, String, Unicode,
                        Float, Date, create_engine)
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base

class DataAccessLayer(object):

    def __init__(self):
        self.engine = None
        self.conn_string = None

    def connect(self):
        self.engine = create_engine(self.conn_string)
        self.Session = sessionmaker(bind=self.engine)

dal = DataAccessLayer()

class Base(object):
    id = Column(Integer, primary_key=True)

Base = declarative_base(cls=Base)

class Item(Base):
    __tablename__ = 'items'

    title = Column(Unicode(255), nullable=False, index=True)
    price = Column(Float, nullable=False)
    per = Column(String(25), nullable=True)
    url = Column(String(255), nullable=True)
    image_url = Column(String(255), nullable=True)
    date = Column(Date, nullable=False)
    vendor = Column(Unicode(50), nullable=False)
