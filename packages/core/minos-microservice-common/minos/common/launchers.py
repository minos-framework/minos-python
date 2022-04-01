from __future__ import (
    annotations,
)

import logging
import warnings
from asyncio import (
    AbstractEventLoop,
    gather,
)
from enum import (
    Enum,
)
from types import (
    ModuleType,
)
from typing import (
    NoReturn,
    Optional,
    Union,
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
from aiomisc_log.enum import (
    DateFormat,
)
from cached_property import (
    cached_property,
)

from .config import (
    Config,
)
from .importlib import (
    get_internal_modules,
    import_module,
)
from .injections import (
    DependencyInjector,
    InjectableMixin,
)
from .ports import (
    Port,
)
from .setup import (
    SetupMixin,
)

logger = logging.getLogger(__name__)


def _create_entrypoint(*args, **kwargs) -> Entrypoint:  # pragma: no cover
    return Entrypoint(*args, **kwargs)


def _create_loop() -> AbstractEventLoop:  # pragma: no cover
    return create_default_event_loop()[0]


class EntrypointLauncher(SetupMixin):
    """EntryPoint Launcher class."""

    def __init__(
        self,
        config: Config,
        injections: list[Union[SetupMixin, type[SetupMixin], str]],
        ports: list[Union[Port, type[Port], str]],
        log_level: Union[int, str] = logging.INFO,
        log_format: Union[str, LogFormat] = "color",
        log_date_format: Union[str, DateFormat] = DateFormat["color"],
        external_modules: Optional[list[ModuleType]] = None,
        external_packages: Optional[list[str]] = None,
        *args,
        **kwargs,
    ):
        if external_modules is None:
            external_modules = list()
        if external_packages is None:
            external_packages = list()

        super().__init__(*args, **kwargs)

        if isinstance(log_date_format, Enum):
            log_date_format = log_date_format.value

        self.config = config

        self._log_level = log_level
        self._log_format = log_format
        self._log_date_format = log_date_format

        self._raw_injections = injections
        self._raw_ports = ports
        self._external_modules = external_modules
        self._external_packages = external_packages

    @classmethod
    def _from_config(cls, *args, config: Config, **kwargs) -> EntrypointLauncher:
        if "injections" not in kwargs:
            kwargs["injections"] = config.get_injections()
        if "ports" not in kwargs:
            kwargs["ports"] = [
                interface["port"] for interface in config.get_interfaces().values() if "port" in interface
            ]
        return cls(config, *args, **kwargs)

    def launch(self) -> NoReturn:
        """Launch a new execution and keeps running forever..

        :return: This method does not return anything.
        """

        basic_config(
            level=self._log_level, log_format=self._log_format, buffered=False, date_format=self._log_date_format
        )

        logger.info("Starting microservice...")

        exception = None
        try:
            self.graceful_launch()
            logger.info("Microservice is up and running!")
            self.loop.run_forever()
        except KeyboardInterrupt as exc:  # pragma: no cover
            logger.info("Stopping microservice...")
            exception = exc
        except Exception as exc:  # pragma: no cover
            logger.exception("Stopping microservice due to an unhandled exception...")
            exception = exc
        finally:
            self.graceful_shutdown(exception)

    def graceful_launch(self) -> None:
        """Launch the execution gracefully.

        :return: This method does not return anything.
        """
        self.loop.run_until_complete(self.setup())

    def graceful_shutdown(self, err: Exception = None) -> None:
        """Shutdown the execution gracefully.

        :return: This method does not return anything.
        """
        self.loop.run_until_complete(self.destroy())

    @cached_property
    def entrypoint(self) -> Entrypoint:
        """Entrypoint instance.

        :return: An ``Entrypoint`` instance.
        """
        return _create_entrypoint(*self.ports, loop=self.loop, log_config=False)

    @cached_property
    def loop(self) -> AbstractEventLoop:
        """Create the loop.

        :return: An ``AbstractEventLoop`` instance.
        """
        return _create_loop()

    @cached_property
    def ports(self) -> list[Port]:
        """List of ports to be launched.

        :return: A list of ``Port`` instances.
        """

        def _fn(raw: Union[Port, type[Port], str]) -> Port:
            if isinstance(raw, str):
                raw = import_module(raw)
            if isinstance(raw, type):
                return raw(config=self.config)
            return raw

        return [_fn(raw) for raw in self._raw_ports]

    @property
    def services(self) -> list[Port]:
        """List of ports to be launched.

        :return: A list of ``Port`` instances.
        """
        warnings.warn("'services' property has been deprecated. Use 'ports' instead.", DeprecationWarning)

        return self.ports

    async def _setup(self) -> None:
        """Wire the dependencies and setup it.

        :return: This method does not return anything.
        """
        modules = self._external_modules + self._internal_modules
        packages = self._external_packages
        self.injector.wire_injections(modules=modules, packages=packages)
        await gather(self.injector.setup_injections(), self.entrypoint.__aenter__())

    @property
    def _internal_modules(self) -> list[ModuleType]:
        return get_internal_modules()

    async def _destroy(self) -> None:
        """Unwire the injected dependencies and destroys it.

        :return: This method does not return anything.
        """
        await gather(self.entrypoint.__aexit__(None, None, None), self.injector.destroy_injections())
        self.injector.unwire_injections()

    @property
    def injections(self) -> dict[str, InjectableMixin]:
        """Get the injections mapping.

        :return: A ``dict`` with injection names as keys and injection instances as values.
        """
        return self.injector.injections

    @cached_property
    def injector(self) -> DependencyInjector:
        """Dependency injector instance.

        :return: A ``DependencyInjector`` instance.
        """
        return DependencyInjector(self.config, self._raw_injections)
