from sqlalchemy import Column, String, Text, Float, Integer, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, backref

Base = declarative_base()


class CartItem(Base):
    __tablename__ = 'items'
    id = Column(Integer, primary_key=True)
    user = Column(String(80))
    quantity = Column(Integer)
    cart_id = Column(Integer, ForeignKey('cart.id'))
    cart = relationship("Cart", backref=backref("items"))


class Cart(Base):
    __tablename__ = 'cart'

    id = Column(Integer, primary_key=True)
    uuid = Column(String(60))
    user = Column(String(80))
    status = Column(Text, nullable=True)
