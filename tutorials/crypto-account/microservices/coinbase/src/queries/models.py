from sqlalchemy.orm import declarative_base

from sqlalchemy import (
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)

Base = declarative_base()


class Wallet(Base):
    __tablename__ = "wallet"

    id = Column(Integer, primary_key=True)
    user = Column(String(80), nullable=False)
    api_key = Column(String(120), nullable=False)
    api_secret = Column(String(120), nullable=False)
