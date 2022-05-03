"""The Aggregate pattern of the Minos Framework."""

__author__ = "Minos Framework Devs"
__email__ = "hey@minos.run"
__version__ = "0.7.0.dev4"

from .actions import (
    Action,
)
from .aggregate import (
    Aggregate,
)
from .collections import (
    IncrementalSet,
    IncrementalSetDiff,
    IncrementalSetDiffEntry,
)
from .contextvars import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
)
from .entities import (
    Entity,
    EntitySet,
    ExternalEntity,
    Ref,
    RefExtractor,
    RefInjector,
    RefResolver,
    RootEntity,
)
from .events import (
    DatabaseEventRepository,
    Event,
    EventDatabaseOperationFactory,
    EventEntry,
    EventRepository,
    FieldDiff,
    FieldDiffContainer,
    IncrementalFieldDiff,
    InMemoryEventRepository,
)
from .exceptions import (
    AggregateException,
    AlreadyDeletedException,
    EventRepositoryConflictException,
    EventRepositoryException,
    NotFoundException,
    RefException,
    SnapshotRepositoryConflictException,
    SnapshotRepositoryException,
    TransactionNotFoundException,
    TransactionRepositoryConflictException,
    TransactionRepositoryException,
    ValueObjectException,
)
from .queries import (
    Condition,
    Ordering,
)
from .snapshots import (
    DatabaseSnapshotRepository,
    InMemorySnapshotRepository,
    SnapshotDatabaseOperationFactory,
    SnapshotEntry,
    SnapshotRepository,
    SnapshotService,
)
from .transactions import (
    TRANSACTION_CONTEXT_VAR,
    DatabaseTransactionRepository,
    InMemoryTransactionRepository,
    TransactionDatabaseOperationFactory,
    TransactionEntry,
    TransactionRepository,
    TransactionService,
    TransactionStatus,
)
from .value_objects import (
    ValueObject,
    ValueObjectSet,
)
