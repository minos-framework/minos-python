"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    Optional,
)

from aiomisc.service.periodic import (
    PeriodicService,
)
from dependency_injector.wiring import (
    Provide,
)

from minos.common import (
    MinosSnapshot,
)


class SnapshotService(PeriodicService):
    """Minos Snapshot Service class."""

    snapshot: MinosSnapshot = Provide["snapshot"]

    def __init__(self, snapshot: Optional[MinosSnapshot] = None, interval: float = 60, **kwargs):
        super().__init__(interval=interval, **kwargs)

        if snapshot is not None:
            self.snapshot = snapshot

    async def start(self) -> None:
        """Start the service execution.

        :return: This method does not return anything.
        """
        await self.snapshot.setup()
        await super().start()

    async def callback(self) -> None:
        """Callback implementation to be executed periodically.

        :return: This method does not return anything.
        """
        await self.snapshot.synchronize()

    async def stop(self, err: Exception = None) -> None:
        """Stop the service execution.

        :param err: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await super().stop(err)
        await self.snapshot.destroy()
