from typing import (
    Any,
    AsyncIterator,
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from sqlalchemy.sql import (
    Executable,
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
    datetime,
)
from minos.transactions import (
    TRANSACTION_CONTEXT_VAR,
    TransactionEntry,
)

from ....operations import (
    SqlAlchemyDatabaseOperation,
)
from .impl import (
    SqlAlchemySnapshotDatabaseOperationFactory,
)


class SqlAlchemySnapshotRepository(DatabaseSnapshotRepository):
    """SqlAlchemy Snapshot Repository class."""

    database_operation_factory: SqlAlchemySnapshotDatabaseOperationFactory

    async def get_table(
        self,
        name: Union[type[Entity], ModelType, str],
        transaction: Optional[TransactionEntry] = None,
        exclude_deleted: bool = True,
    ) -> Subquery:
        """Get a table relative to the given ``Entity``.

        :param name: Class name of the ``Entity``.
        :param transaction: The transaction within the operation is performed. If not any value is provided, then the
            transaction is extracted from the context var. If not any transaction is being scoped then the query is
            performed to the global snapshot.
        :param exclude_deleted: If ``True``, deleted ``Entity`` entries are included, otherwise deleted
            ``Entity`` entries are filtered.
        :return: A pre-filtered table as a ``Subquery`` instance.
        """

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

    def execute_statement(self, statement: Executable, **kwargs) -> AsyncIterator[dict[str, Any]]:
        """Execute given statement and fetch results asynchronously.

        :param statement: The statement to be executed.
        :param kwargs: Additional named arguments.
        :return: An ``AsyncIterator`` instance containing ``dict`` instances.
        """
        operation = SqlAlchemyDatabaseOperation(statement)
        return self.execute_on_database_and_fetch_all(operation, **kwargs)

    def _build_entry(
        self,
        _type: str,
        _transaction_uuid: UUID,
        _deleted: bool,
        uuid: UUID,
        version: int,
        created_at: datetime,
        updated_at: datetime,
        **data
    ) -> SnapshotEntry:
        if _deleted:
            data = None

        return SnapshotEntry(
            name=_type,
            transaction_uuid=_transaction_uuid,
            uuid=uuid,
            version=version,
            created_at=created_at,
            updated_at=updated_at,
            data=data,
        )
