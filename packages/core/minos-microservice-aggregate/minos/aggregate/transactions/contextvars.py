from __future__ import (
    annotations,
)

from contextvars import (
    ContextVar,
)
from typing import (
    TYPE_CHECKING,
    Final,
    Optional,
)

if TYPE_CHECKING:
    from .entries import (
        TransactionEntry,
    )

TRANSACTION_CONTEXT_VAR: Final[ContextVar[Optional[TransactionEntry]]] = ContextVar("transaction", default=None)
"""Context Variable that contains the identifier of the currently active transaction, or ``None`` otherwise."""
