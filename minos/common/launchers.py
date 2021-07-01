"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from asyncio import (
    AbstractEventLoop,
)
from typing import (
    NoReturn,
    Type,
    Union,
)

from aiomisc import (
    Service,
    receiver,
)
from aiomisc.entrypoint import (
    Entrypoint,
)
from aiomisc.log import (
    LogFormat,
    basic_config,
)
from aiomisc.utils import (
    create_default_event_loop,
)
from cached_property import (
    cached_property,
)

from .configuration import (
    MinosConfig,
)
from .importlib import (
    import_module,
)
from .injectors import (
    DependencyInjector,
)
from .setup import (
    MinosSetup,
)

logger = logging.getLogger(__name__)


def _create_entrypoint(*args, **kwargs) -> Entrypoint:  # pragma: no cover
    return Entrypoint(*args, **kwargs)


def _create_loop() -> AbstractEventLoop:  # pragma: no cover
    return create_default_event_loop()[0]


class EntrypointLauncher(MinosSetup):
    """EntryPoint Launcher class."""

    def __init__(
        self,
        config: MinosConfig,
        injections: dict[str, Union[MinosSetup, Type[MinosSetup], str]],
        services: list[Union[Service, Type[Service], str]],
        interval: float = 0.1,
        log_level: Union[int, str] = logging.INFO,
        log_format: Union[str, LogFormat] = "color",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.config = config
        self.interval = interval

        self._log_level = log_level
        self._log_format = log_format

        self._raw_injections = injections
        self._raw_services = services

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> EntrypointLauncher:
        if "injections" not in kwargs:
            kwargs["injections"] = config.service.injections
        if "services" not in kwargs:
            kwargs["services"] = config.service.services
        return cls(config, *args, **kwargs)

    def launch(self) -> NoReturn:
        """Launch a new execution and keeps running forever..

        :return: This method does not return anything.
        """

        basic_config(
            level=self._log_level, log_format=self._log_format, buffered=False,
        )

        logger.info("Starting microservice...")

        try:
            with self.entrypoint:
                logger.info("Microservice is up and running!")
                self.loop.run_forever()
        except KeyboardInterrupt:  # pragma: no cover
            logger.info("Stopping microservice...")
        finally:
            self.graceful_shutdown()

    def graceful_shutdown(self, err: Exception = None) -> NoReturn:
        """Shutdown the services execution gracefully.

        :return: This method does not return anything.
        """
        self.loop.run_until_complete(self.entrypoint.graceful_shutdown(err))

    @cached_property
    def entrypoint(self) -> Entrypoint:
        """Entrypoint instance.

        :return: An ``Entrypoint`` instance.
        """

        # noinspection PyUnusedLocal
        @receiver(Entrypoint.PRE_START)
        async def _start(*args, **kwargs):
            await self.setup()

        # noinspection PyUnusedLocal
        @receiver(Entrypoint.POST_STOP)
        async def _stop(*args, **kwargs):
            await self.destroy()

        return _create_entrypoint(*self.services, loop=self.loop, log_config=False)

    @cached_property
    def loop(self) -> AbstractEventLoop:
        """Create the loop.

        :return: An ``AbstractEventLoop`` instance.
        """
        return _create_loop()

    @cached_property
    def services(self) -> list[Service]:
        """List of services to be launched.

        :return: A list of ``Service`` instances.
        """
        kwargs = {"config": self.config, "interval": self.interval}

        def _fn(raw: Union[Service, Type[Service], str]) -> Service:
            if isinstance(raw, str):
                raw = import_module(raw)
            if isinstance(raw, type):
                return raw(**kwargs)
            return raw

        return [_fn(raw) for raw in self._raw_services]

    async def _setup(self) -> NoReturn:
        """Wire the dependencies and setup it.

        :return: This method does not return anything.
        """
        await self.injector.wire(modules=self._internal_modules)

    @property
    def _internal_modules(self):
        from minos import (
            common,
        )

        modules = [common]
        try:
            # noinspection PyUnresolvedReferences
            from minos import (
                networks,
            )

            modules += [networks]  # pragma: no cover
        except ImportError:
            pass

        try:
            # noinspection PyUnresolvedReferences
            from minos import (
                saga,
            )

            modules += [saga]  # pragma: no cover
        except ImportError:
            pass
        return modules

    async def _destroy(self) -> NoReturn:
        """Unwire the injected dependencies and destroys it.

        :return: This method does not return anything.
        """
        await self.injector.unwire()

    @cached_property
    def injector(self) -> DependencyInjector:
        """Dependency injector instance.

        :return: A ``DependencyInjector`` instance.
        """
        return DependencyInjector(config=self.config, **self._raw_injections)
