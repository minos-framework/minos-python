from minos.common import MinosSetup, MinosConfig
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.queries.models import Base


class NotifierQueryServiceRepository(MinosSetup):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = create_engine("postgresql+psycopg2://postgres:@localhost:5432/notifier_query_db".format(**kwargs))
        self.session = sessionmaker(bind=self.engine)

    async def _setup(self) -> None:
        Base.metadata.create_all(self.engine)

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs):
        return cls(*args, **(config.query_repository._asdict()) | kwargs)

    @property
    def session(self):
        return self.session
