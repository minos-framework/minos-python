from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    Entity,
    RootEntity,
    ValueObject,
    ValueObjectSet,
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
    async def get_product(uuid: UUID) -> Product:
        """Get product"""
        product = await Product.get(uuid)
        return product

    @staticmethod
    async def create_product(data: {}) -> UUID:
        """Create a new instance."""
        price = Price(**data["price"])
        data["price"] = price
        cat_list = []
        if "categories" in data:
            for category in data["categories"]:
                category_object = Category(**category)
                cat_list.append(category_object)
        data["categories"] = set(cat_list)
        root = await Product.create(**data)
        return root.uuid

    @staticmethod
    async def delete_product(uid) -> UUID:
        """Create a new instance."""
        product = await Product.get(uid)
        await product.delete()
        return product.uuid
