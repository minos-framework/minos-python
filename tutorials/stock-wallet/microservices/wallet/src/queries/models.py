from sqlalchemy import (
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    DateTime,
)
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlalchemy.orm import (
    backref,
    declarative_base,
    relationship,
)

Base = declarative_base()


class Wallet(Base):
    __tablename__ = "wallet"
    id = Column(Integer, primary_key=True)
    uuid = Column("uuid", UUID_PG(as_uuid=True))
    name = Column(String(40), unique=True)


class Ticker(Base):
    __tablename__ = "ticker"
    id = Column(Integer, primary_key=True)
    uuid = Column("uuid", UUID_PG(as_uuid=True))
    wallet_id = Column(Integer, ForeignKey("wallet.id"))
    wallet = relationship("Wallet", backref=backref("tickers"))
    ticker = Column(String(10), unique=True)
    is_crypto = Column(Boolean, default=False, unique=False)
    latest_value = Column(Float)


class Quotes(Base):
    __tablename__ = "quotes"
    id = Column(Integer, primary_key=True)
    close_value = Column(Float)
    volume = Column(Integer)
    when = Column(DateTime)
    ticker_id = Column(Integer, ForeignKey("ticker.id"))
    ticker = relationship("Ticker", backref=backref("quotes"))
