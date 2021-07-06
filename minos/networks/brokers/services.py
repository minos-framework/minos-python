"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from aiomisc.service.periodic import (
    PeriodicService,
)
from cached_property import (
    cached_property,
)

from .producers import (
    Producer,
)


class ProducerService(PeriodicService):
    """Minos QueueDispatcherService class."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_kwargs = kwargs

    async def start(self) -> None:
        """Method to be called at the startup by the internal ``aiomisc`` loigc.

        :return: This method does not return anything.
        """
        await self.dispatcher.setup()
        await super().start()

    async def callback(self) -> None:
        """Method to be called periodically by the internal ``aiomisc`` logic.

        :return:This method does not return anything.
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
    def dispatcher(self) -> Producer:
        """Get the service dispatcher.

        :return: A ``Producer`` instance.
        """
        return Producer.from_config(**self._init_kwargs)
