from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    RootEntity, Entity, ValueObject, ValueObjectSet
)


class Price(Entity):
    currency: str
    units: int


class Category(ValueObject):
    title: str


class Product(RootEntity):
    """Product RootEntity class."""
    title: str
    description: str
    picture: str
    price: Price
    categories: ValueObjectSet[Category]


class ProductAggregate(Aggregate[Product]):
    """ProductAggregate class."""

    @staticmethod
    async def getProduct(uid) -> Product:
        product = await Product.get(uid)
        return product

    @staticmethod
    async def createProduct(data: {}) -> UUID:
        """Create a new instance."""
        price = Price(**data['price'])
        data['price'] = price
        if 'categories' in data:
            cat_list = []
            for category in data['categories']:
                category_object = Category(**category)
                cat_list.append(category_object)
        data['categories'] = set(cat_list)
        root = await Product.create(**data)
        return root.uuid

    @staticmethod
    async def deleteProduct(uid) -> UUID:
        """Create a new instance."""
        product = await Product.get(uid)
        product.delete()
        return product.uuid
