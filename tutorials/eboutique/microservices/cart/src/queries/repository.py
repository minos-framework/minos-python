from sqlalchemy import (
    create_engine,
)
from sqlalchemy.orm import (
    sessionmaker
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
    Cart, Product, CartItem
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
        self.session.commit()

    def add_item(self, cart_uuid: str, product: dict, item: dict):
        cart_obj = self.session.query(Cart).filter(Cart.uuid == cart_uuid).first()
        if cart_obj is not None:
            # check if the product already exist
            product_obj = self.session.query(Product).filter(Product.uuid == product['uuid']).first()
            if product_obj is None:
                product_obj = Product(uuid=product['uuid'], title=product['title'], picture=product['picture'])
                self.session.add(product_obj)
            item_obj = CartItem(uuid=item['uuid'], quantity=item['quantity'], cart=cart_obj, product=product_obj)
            self.session.add(item_obj)
            self.session.commit()
        else:
            raise Exception

    def get(self, uuid):
        cart_obj = self.session.query(Cart).filter(Cart.uuid == uuid).first()
        if cart_obj is not None:
            return {'uuid': cart_obj.uuid, 'user': cart_obj.user, 'status': cart_obj.status}
        else:
            return None

    def get_items_cart(self, cart_uuid):
        cart_object = self.session.query(Cart).filter(Cart.uuid == cart_uuid).first()
        query = self.session.query(CartItem).filter(CartItem.cart == cart_object)
        count = query.count()
        if count > 0:
            cart_items_obj = query.all()
            cart_items = []
            for item in cart_items_obj:
                cart_items.append({'uuid': item.uuid, 'quantity': item.quantity,
                                   'product': {'uuid': item.product.uuid, 'title': item.product.title}
                                   })
            return cart_items
        else:
            return None
