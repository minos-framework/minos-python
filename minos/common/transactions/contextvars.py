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
    from .models import (
        Transaction,
    )

TRANSACTION_CONTEXT_VAR: Final[ContextVar[Optional[Transaction]]] = ContextVar("transaction", default=None)
