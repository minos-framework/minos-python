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
    Quotes,
    Ticker,
    Wallet,
)


class WalletQueryServiceRepository(MinosSetup):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = create_engine("postgresql+psycopg2://minos:min0s@postgres:5432/wallet_query_db".format(**kwargs))
        Session = sessionmaker(bind=self.engine, autocommit=True)
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
        return wallet.id

    def get_wallets(self):
        wallets_query = self.session.query(Wallet).all()
        wallets = []
        for wallet in wallets_query:
            wallets.append({"wallet_name": wallet.name, "uuid": wallet.uuid})
        return wallets

    def get_wallet(self, uuid):
        wallet_query = self.session.query(Wallet).filter(Wallet.uuid == uuid).first()
        return {"id": wallet_query.id, "uuid": wallet_query.uuid, "wallet_name": wallet_query.name}

    def add_tickers(self, wallet_uuid: str, ticker: dict):
        wallet = self.session.query(Wallet).filter(Wallet.uuid == wallet_uuid).first()
        ticker = Ticker(uuid=ticker["uuid"], ticker=ticker["ticker"], flag=ticker["flag"], wallet=wallet)
        self.session.add(ticker)

    def get_tickers(self, wallet_uuid):
        wallet = self.session.query(Wallet).filter(Wallet.uuid == wallet_uuid).first()
        tickers_query = self.session.query(Ticker).filter(Ticker.wallet == wallet).all()
        tickers = []
        for ticker in tickers_query:
            tickers.append(
                {
                    "ticker": ticker.ticker,
                    "uuid": ticker.uuid,
                    "flag": ticker.flag,
                    "latest_value": ticker.latest_value,
                }
            )
        return tickers

    def get_quotes(self, ticker_uuid):
        ticker = self.session.query(Ticker).filter(Ticker.uuid == ticker_uuid).first()
        quotes_query = self.session.query(Quotes).filter(Quotes.ticker == ticker).order_by(Quotes.when.desc()).all()
        quotes = []
        for quote in quotes_query:
            quotes.append([quote.when, quote.close_value])
        return quotes

    def add_quote(self, ticker: str, close: float, volume: int, when: str):
        ticker = self.session.query(Ticker).filter(Ticker.ticker == ticker).first()
        quote = Quotes()
        quote.ticker = ticker
        quote.when = when
        quote.volume = volume
        quote.close_value = close
        self.session.add(quote)
