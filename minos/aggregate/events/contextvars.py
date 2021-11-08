from contextvars import (
    ContextVar,
)
from typing import (
    Final,
)

SUBMITTING_EVENT_CONTEXT_VAR: Final[ContextVar[bool]] = ContextVar("submitting_event", default=False)
