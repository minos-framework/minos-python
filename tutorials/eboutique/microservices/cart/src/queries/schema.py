from marshmallow_sqlalchemy import SQLAlchemyAutoSchema

from src.queries.models import Product, CartItem, Cart


class ProductSchema(SQLAlchemyAutoSchema):
    class Meta:
        model: Product
        load_instance = True
        include_relationships = True


class CartItemSchema(SQLAlchemyAutoSchema):
    class Meta:
        model: CartItem
        load_instance = True
        include_fk = True


class CartSchema(SQLAlchemyAutoSchema):
    class Meta:
        model: Cart
        load_instance = True
        include_relationships = True
