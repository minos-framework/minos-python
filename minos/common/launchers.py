"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from typing import (
    NoReturn,
)

from aiomisc import (
    Service,
    entrypoint,
    receiver,
)
from aiomisc.entrypoint import (
    Entrypoint,
)
from cached_property import (
    cached_property,
)

from .injectors import (
    DependencyInjector,
)
from .setup import (
    MinosSetup,
)

logger = logging.getLogger(__name__)


class EntrypointLauncher(MinosSetup):
    """EntryPoint Launcher class."""

    def __init__(self, services: list[Service], injector: DependencyInjector, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.services = services
        self.injector = injector

    def launch(self) -> NoReturn:
        """Launch a new execution and keeps running forever..

        :return: This method does not return anything.
        """
        logger.info("Starting microservice...")
        with self.entrypoint as loop:
            logger.info("Microservice is up and running!")
            loop.run_forever()

    @cached_property
    def entrypoint(self) -> Entrypoint:
        """Entrypoint instance.

        :return: An ``Entrypoint`` instance.
        """

        # noinspection PyUnusedLocal
        @receiver(entrypoint.PRE_START)
        async def _start(*args, **kwargs):
            await self.setup()

        # noinspection PyUnusedLocal
        @receiver(entrypoint.POST_STOP)
        async def _stop(*args, **kwargs):
            await self.destroy()

        return entrypoint(*self.services)

    async def _setup(self) -> NoReturn:
        """Wire the dependencies and setup it.

        :return: This method does not return anything.
        """
        from minos import (
            common,
        )

        modules = [common]
        try:
            from minos import (
                networks,
            )

            modules += [networks]  # pragma: no cover
        except ImportError:
            pass

        try:
            from minos import (
                saga,
            )

            modules += [saga]  # pragma: no cover
        except ImportError:
            pass
        await self.injector.wire(modules=modules)

    async def _destroy(self) -> NoReturn:
        """Unwire the injected dependencies and destroys it.

        :return: This method does not return anything.
        """
        await self.injector.unwire()
