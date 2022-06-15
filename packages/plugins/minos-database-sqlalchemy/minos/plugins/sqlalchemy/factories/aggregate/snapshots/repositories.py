from typing import (
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from sqlalchemy.sql import (
    Subquery,
)

from minos.aggregate import (
    DatabaseSnapshotRepository,
    Entity,
    SnapshotEntry,
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
        self,
        name: Union[type[Entity], ModelType, str],
        transaction: Optional[TransactionEntry] = None,
        exclude_deleted: bool = True,
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

    def _build_entry(self, _type: str, _transaction_uuid: UUID, _deleted: bool, **kwargs) -> SnapshotEntry:
        return SnapshotEntry(type_=_type, transaction_uuid=_transaction_uuid, deleted=_deleted, **kwargs)
