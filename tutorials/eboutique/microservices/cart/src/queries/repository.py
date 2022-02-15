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
    Cart,
)


class CartQueryRepository(MinosSetup):
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
        return cls(*args, **(config.repository._asdict() | {"database": "cart_query_db"}) | kwargs)

    def add(self, event: Event):

        cart = Cart()
        cart.uuid = event["uuid"]
        cart.status = event.get_one("status")
        cart.user = event.get_one("user")
        self.session.add(cart)

    def add_item(self, event: Event):
        ...

    def get(self, uuid):
        cart_obj = self.session.query(Cart).filter(Cart.uuid == uuid).first()
        if cart_obj is not None:
            row_as_dict = {column: str(getattr(cart_obj, column)) for column in cart_obj.__table__.c.keys()}
            return row_as_dict
        else:
            return None
