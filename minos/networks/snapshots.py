"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from aiomisc.service.periodic import (
    PeriodicService,
)
from cached_property import (
    cached_property,
)

from minos.common import (
    PostgreSqlSnapshotBuilder,
)


class SnapshotService(PeriodicService):
    """Minos Snapshot Service class."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_kwargs = kwargs

    async def start(self) -> None:
        """Start the service execution.

        :return: This method does not return anything.
        """
        await self.dispatcher.setup()
        await super().start()

    async def callback(self) -> None:
        """Callback implementation to be executed periodically.

        :return: This method does not return anything.
        """
        await self.dispatcher.dispatch()

    async def stop(self, err: Exception = None) -> None:
        """Stop the service execution.

        :param err: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await super().stop(err)
        await self.dispatcher.destroy()

    @cached_property
    def dispatcher(self) -> PostgreSqlSnapshotBuilder:
        """Get the service dispatcher.

        :return: A ``PostgreSqlSnapshotBuilder`` instance.
        """
        return PostgreSqlSnapshotBuilder.from_config(**self._init_kwargs)
