from sqlalchemy import (
    create_engine,
)
from sqlalchemy.orm import (
    sessionmaker,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
)

from src.queries.models import (
    Base,
    Ticker,
    Wallet,
)


class WalletQueryServiceRepository(MinosSetup):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = create_engine("postgresql+psycopg2://minos:min0s@postgres:5432/wallet_query_db".format(**kwargs))
        Session = sessionmaker(bind=self.engine)
        self._session = Session()

    async def _setup(self) -> None:
        Base.metadata.create_all(self.engine)

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs):
        return cls(*args, **(config.query_repository._asdict()) | kwargs)

    @property
    def session(self):
        return self._session

    def create_wallet(self, name: str, uuid: str):
        wallet = Wallet()
        wallet.name = name
        wallet.uuid = uuid
        self.session.add(wallet)
        self.session.commit()
        return wallet.id

    def get_wallets(self):
        wallets_query = self.session.query(Wallet).all()
        wallets = []
        for wallet in wallets_query:
            wallets.append({"name": wallet.name, "uuid": wallet.uuid})
        return wallets

    def get_wallet(self, uuid):
        wallet_query = self.session.query(Wallet).filter(Wallet.uuid == uuid).first()
        return {"id": wallet_query.id, "uuid": wallet_query.uuid, "name": wallet_query.name}

    def add_tickers(self, wallet_uuid: str, ticker: dict):
        wallet = self.session.query(Wallet).filter(Wallet.uuid == wallet_uuid).first()
        ticker = Ticker(uuid=ticker["uuid"], ticker=ticker["ticker"], is_crypto=ticker["is_crypto"], wallet=wallet)
        self.session.add(ticker)
        self.session.commit()

    def get_tickers(self, wallet_uuid):
        wallet = self.session.query(Wallet).filter(Wallet.uuid == wallet_uuid).first()
        tickers_query = self.session.query(Ticker).filter(Ticker.wallet == wallet).all()
        tickers = []
        for ticker in tickers_query:
            tickers.append(
                {
                    "ticker": ticker.ticker,
                    "uuid": ticker.uuid,
                    "is_crypto": ticker.is_crypto,
                    "latest_value": ticker.latest_value,
                }
            )
        return tickers
