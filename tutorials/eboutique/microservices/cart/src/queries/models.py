from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlalchemy.orm import (
    backref,
    declarative_base,
    relationship,
)

Base = declarative_base()


class Product(Base):
    __tablename__ = "product"
    id = Column(Integer, primary_key=True)
    uuid = Column("uuid", UUID_PG(as_uuid=True))
    title = Column(String(120))
    picture = Column(String(120))


class CartItem(Base):
    __tablename__ = "items"
    id = Column(Integer, primary_key=True)
    uuid = Column("uuid", UUID_PG(as_uuid=True))
    quantity = Column(Integer)
    cart_id = Column(Integer, ForeignKey("cart.id"))
    cart = relationship("Cart", backref=backref("items"))
    product_id = Column(Integer, ForeignKey("product.id"))
    product = relationship("Product", backref=backref("items"))


class Cart(Base):
    __tablename__ = "cart"
    id = Column(Integer, primary_key=True)
    uuid = Column("uuid", UUID_PG(as_uuid=True))
    user = Column(String(80))
    status = Column(Text, nullable=True)
