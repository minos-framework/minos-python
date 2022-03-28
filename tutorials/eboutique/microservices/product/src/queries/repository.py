from sqlalchemy import (
    create_engine,
)
from sqlalchemy.orm import (
    sessionmaker,
)

from minos.aggregate import (
    Event,
)
from minos.common import (
    MinosConfig,
    MinosSetup,
)

from .models import (
    Base,
    Categories,
    Price,
    Product,
)


class ProductQueryRepository(MinosSetup):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(**kwargs))
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    async def _setup(self) -> None:
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs):
        return cls(*args, **(config.repository._asdict() | {"database": "product_query_db"}) | kwargs)

    def add(self, event: Event):

        product = Product()
        product.uuid = event["uuid"]
        product.title = event.get_one("title")
        product.description = event.get_one("description")
        product.picture = event.get_one("picture")
        price_object = event.get_one("price")
        price: Price = Price()
        price.currency = price_object.currency
        price.units = price_object.units
        product.price = price
        product_categories = []

        for category_obj in event.get_one("categories"):
            category = Categories()
            category.title = category_obj.title
            product_categories.append(category)

        product.categories = product_categories
        self.session.add(product)
        self.session.commit()

    def get_all(self):
        results = []
        for row in self.session.query(Product).all():
            row_as_dict = {column: str(getattr(row, column)) for column in row.__table__.c.keys()}
            results.append(row_as_dict)
        return results

    def get(self, uuid) -> dict:
        product = self.session.query(Product).filter(Product.uuid == uuid).first()
        row_as_dict = {column: str(getattr(product, column)) for column in product.__table__.c.keys()}
        return row_as_dict
