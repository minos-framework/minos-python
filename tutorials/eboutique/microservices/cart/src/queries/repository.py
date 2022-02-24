import logging

from sqlalchemy import (
    create_engine,
)
from sqlalchemy.orm import (
    sessionmaker, subqueryload,
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
    Cart, Product, CartItem,
)

logger = logging.getLogger(__name__)

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
        self.session.commit()

    def add_item(self, cart_uuid: str, product: dict, item: dict ):
        logger.warning(product)
        logger.warning(item)
        cart_obj = self.session.query(Cart).filter(Cart.uuid == cart_uuid).first()
        if cart_obj is not None:
            # check if the product already exist
            product_obj = self.session.query(Product).filter(Product.uuid == product['uuid']).first()
            if product_obj is None:
                product_obj = Product(uuid=product['uuid'], title=product['title'], picture=product['picture'])
                self.session.add(product_obj)
                self.session.commit()
            item_obj = CartItem(uuid=item['uuid'], quantity=item['quantity'], cart=cart_obj, product=product_obj)
            self.session.add(item_obj)
            self.session.commit()
        else:
            raise Exception

    def get(self, uuid):
        cart_obj = self.session.query(Cart).filter(Cart.uuid == uuid).first()
        if cart_obj is not None:
            return cart_obj.to_dict()
        else:
            return None

    def get_items_cart(self, cart_uuid):
        product = self.session.query(Cart).filter(Cart.uuid == cart_uuid).first()
        cart_obj = self.session.query(CartItem).filter(CartItem.product == product).all()
        if len(cart_obj) > 0:
            return cart_obj
        else:
            return None
