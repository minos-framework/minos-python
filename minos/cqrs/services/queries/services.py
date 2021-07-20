"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from collections import (
    Callable,
)

from minos.common import (
    AggregateDiff,
)

from ..abc import (
    Service,
)
from .handlers import (
    _build_event_saga,
)


class QueryService(Service):
    """Query Service class"""

    async def _handle_event(self, diff: AggregateDiff, fn: Callable):
        definition = _build_event_saga(diff, self, fn)
        return await self.saga_manager.run(definition=definition, asynchronous=False, raise_on_error=True)
