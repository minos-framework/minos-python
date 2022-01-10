__author__ = """Clariteia Devs"""
__email__ = "devs@clariteia.com"
__version__ = "0.2.3"

from .contextvars import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
)
from .events import (
    EventEntry,
    EventRepository,
    InMemoryEventRepository,
    PostgreSqlEventRepository,
)
from .exceptions import (
    AggregateException,
    AggregateNotFoundException,
    DeletedAggregateException,
    EventRepositoryConflictException,
    EventRepositoryException,
    SnapshotRepositoryConflictException,
    SnapshotRepositoryException,
    TransactionNotFoundException,
    TransactionRepositoryConflictException,
    TransactionRepositoryException,
    ValueObjectException,
)
from .models import (
    Action,
    Aggregate,
    AggregateDiff,
    AggregateRef,
    Entity,
    EntitySet,
    FieldDiff,
    FieldDiffContainer,
    IncrementalFieldDiff,
    IncrementalSet,
    IncrementalSetDiff,
    IncrementalSetDiffEntry,
    ModelRef,
    ModelRefExtractor,
    ModelRefInjector,
    ModelRefResolver,
    ValueObject,
    ValueObjectSet,
)
from .queries import (
    Condition,
    Ordering,
)
from .snapshots import (
    InMemorySnapshotRepository,
    PostgreSqlSnapshotQueryBuilder,
    PostgreSqlSnapshotReader,
    PostgreSqlSnapshotRepository,
    PostgreSqlSnapshotSetup,
    PostgreSqlSnapshotWriter,
    SnapshotEntry,
    SnapshotRepository,
    SnapshotService,
)
from .transactions import (
    TRANSACTION_CONTEXT_VAR,
    InMemoryTransactionRepository,
    PostgreSqlTransactionRepository,
    TransactionEntry,
    TransactionRepository,
    TransactionService,
    TransactionStatus,
)
