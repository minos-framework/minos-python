from typing import (
    Optional,
)

from sqlalchemy.sql import (
    Subquery,
)

from minos.aggregate import (
    DatabaseSnapshotRepository,
    Entity,
)
from minos.common import (
    NULL_UUID,
    ModelType,
    classname,
)
from minos.transactions import (
    TRANSACTION_CONTEXT_VAR,
    TransactionEntry,
)

from .impl import (
    SqlAlchemySnapshotDatabaseOperationFactory,
)


class SqlAlchemySnapshotRepository(DatabaseSnapshotRepository):
    """TODO"""

    database_operation_factory: SqlAlchemySnapshotDatabaseOperationFactory

    async def get_table(
        self, name: type[Entity], transaction: Optional[TransactionEntry] = None, exclude_deleted: bool = True
    ) -> Subquery:
        """TODO"""

        if isinstance(name, ModelType):
            name = name.model_cls
        if isinstance(name, type):
            name = classname(name)

        if transaction is None:
            transaction = TRANSACTION_CONTEXT_VAR.get()

        if transaction is None:
            transaction_uuids = (NULL_UUID,)
        else:
            transaction_uuids = await transaction.uuids

        return self.database_operation_factory.get_table(name, transaction_uuids, exclude_deleted)
