from sqlalchemy import Column, Integer, String, Text, Float, TIMESTAMP, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from pgvector.sqlalchemy import Vector

Base = declarative_base()

class TickerDescribe(Base):
    __tablename__ = 'ticker_describe'
    ticker = Column(String(50), primary_key=True)
    sector = Column(String(50))
    name = Column(String(70))
    describe = Column(Text)

class PriceHour(Base):
    __tablename__ = 'price_hour'
    open = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)
    date = Column(TIMESTAMP, nullable=False)
    ticker = Column(String(50), nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('ticker', 'date'),
    )

class News(Base):
    __tablename__ = 'news'
    id = Column(Integer, primary_key=True, autoincrement=True)
    channel = Column(String(50))
    date = Column(TIMESTAMP)
    text = Column(Text)

class NewsEmbedd(Base):
    __tablename__ = 'news_embedd'
    id = Column(Integer, primary_key=True)
    embedding = Column(Vector(768))

class NewsWithTicker(Base):
    __tablename__ = 'news_with_ticker'
    id = Column(Integer, primary_key=True, autoincrement=True)
    channel = Column(String(50))
    ticker = Column(String(50))
    date = Column(TIMESTAMP)
    text = Column(Text)

class NewsWithTickerEmbedd(Base):
    __tablename__ = 'news_with_ticker_embedd'
    id = Column(Integer, primary_key=True)
    embedding = Column(Vector(768))