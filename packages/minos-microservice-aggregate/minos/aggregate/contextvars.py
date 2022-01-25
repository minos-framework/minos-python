from contextvars import (
    ContextVar,
)
from typing import (
    Final,
)

IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR: Final[ContextVar[bool]] = ContextVar(
    "is_repository_serialization", default=False
)
