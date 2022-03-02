from minos.common import (
    MinosConfig,
    MinosSetup,
)
from sqlalchemy import (
    create_engine,
)
from sqlalchemy.orm import (
    sessionmaker,
)

from src.queries.models import (
    Base,
    Wallet,
)


class CoinbaseQueryServiceRepository(MinosSetup):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = create_engine("postgresql+psycopg2://postgres:@localhost:5432/coinbase_query_db")
        self.session = sessionmaker(bind=self.engine)

    async def _setup(self) -> None:
        Base.metadata.create_all(self.engine)

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CoinbaseQueryRepository:
        return cls(*args, **(config.query_repository._asdict()) | kwargs)

    def add_wallet(self, user: str, api_key: str, api_secret: str):
        wallet = Wallet()
        wallet.user = user
        wallet.api_key = api_key
        wallet.api_secret = api_secret
        self.session.add(wallet)
