from contextvars import (
    ContextVar,
)
from typing import (
    Final,
    Optional,
)

REQUEST_REPLY_TOPIC_CONTEXT_VAR: Final[ContextVar[Optional[str]]] = ContextVar("reply_topic", default=None)
REQUEST_HEADERS_CONTEXT_VAR: Final[ContextVar[Optional[dict[str, str]]]] = ContextVar("headers", default=None)
