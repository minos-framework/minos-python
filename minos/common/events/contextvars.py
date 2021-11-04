from contextvars import (
    ContextVar,
)
from typing import (
    Final,
)

IS_SUBMITTING_EVENT_CONTEXT_VAR: Final[ContextVar[bool]] = ContextVar("is_submitting_event", default=False)
