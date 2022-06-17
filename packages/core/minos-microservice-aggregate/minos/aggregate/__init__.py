"""The Aggregate pattern of the Minos Framework."""

__author__ = "Minos Framework Devs"
__email__ = "hey@minos.run"
__version__ = "0.8.0.dev3"

from .actions import (
    Action,
)
from .collections import (
    IncrementalSet,
    IncrementalSetDiff,
    IncrementalSetDiffEntry,
)
from .contextvars import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
)
from .deltas import (
    DatabaseDeltaRepository,
    Delta,
    DeltaDatabaseOperationFactory,
    DeltaEntry,
    DeltaRepository,
    FieldDiff,
    FieldDiffContainer,
    IncrementalFieldDiff,
    InMemoryDeltaRepository,
)
from .entities import (
    Entity,
    EntityRepository,
    EntitySet,
    Ref,
    RefExtractor,
    RefInjector,
    RefResolver,
)
from .exceptions import (
    AggregateException,
    AlreadyDeletedException,
    DeltaRepositoryConflictException,
    DeltaRepositoryException,
    NotFoundException,
    RefException,
    SnapshotRepositoryConflictException,
    SnapshotRepositoryException,
    ValueObjectException,
)
from .impl import (
    Aggregate,
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
from .value_objects import (
    ValueObject,
    ValueObjectSet,
)
