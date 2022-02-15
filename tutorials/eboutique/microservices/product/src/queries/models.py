from sqlalchemy import (
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import (
    backref,
    declarative_base,
    relationship,
)

Base = declarative_base()


class Price(Base):
    __tablename__ = "price"

    id = Column(Integer, primary_key=True)
    uuid = Column(String(30))
    currency = Column(String)
    units = Column(Float)


class Categories(Base):
    __tablename__ = "categories"

    id = Column(Integer, primary_key=True)
    title = Column(String)
    product_id = Column(Integer, ForeignKey("product.id"))
    product = relationship("Product", backref=backref("categories"))


class Product(Base):
    __tablename__ = "product"

    id = Column(Integer, primary_key=True)
    uuid = Column(String(60))
    title = Column(String(80))
    description = Column(Text, nullable=True)
    picture = Column(String(120), nullable=True)
    price_id = Column(Integer, ForeignKey("price.id"))
    price = relationship("Price", backref=backref("product", uselist=False))
