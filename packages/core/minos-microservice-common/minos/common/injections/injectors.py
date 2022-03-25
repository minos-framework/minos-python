from __future__ import (
    annotations,
)

from asyncio import (
    gather,
)
from typing import (
    TYPE_CHECKING,
    Optional,
    Type,
    Union,
)

from cached_property import (
    cached_property,
)
from dependency_injector import (
    containers,
    providers,
)

from ..importlib import (
    import_module,
)
from .mixins import (
    InjectableMixin,
)

if TYPE_CHECKING:
    from ..config import (
        Config,
    )


class DependencyInjector:
    """Async wrapper of ``dependency_injector.containers.Container``."""

    def __init__(
        self,
        config: Config,
        injections: Optional[list[Union[InjectableMixin, Type[InjectableMixin], str]]] = None,
    ):
        if injections is None:
            injections = list()
        self.config = config
        self._raw_injections = injections

    @cached_property
    def injections(self) -> dict[str, InjectableMixin]:
        """Get the injections' dictionary.

        :return: A dict of injections.
        """
        injections = dict()

        def _fn(raw: Union[InjectableMixin, type[InjectableMixin], str]) -> InjectableMixin:
            if isinstance(raw, str):
                raw = import_module(raw)
            if isinstance(raw, type):
                return raw.from_config(self.config, **injections)
            return raw

        for value in self._raw_injections:
            try:
                value = _fn(value)
                key = value.get_injectable_name()
            except Exception as exc:
                raise ValueError(f"An exception was raised while building injections: {exc!r}")
            injections[key] = value

        return injections

    async def wire_and_setup_injections(self, *args, **kwargs) -> None:
        """Connect the configuration.

        :return: This method does not return anything.
        """

        self.wire_injections(*args, **kwargs)
        await self.setup_injections()

    async def unwire_and_destroy_injections(self) -> None:
        """Disconnect the configuration.

        :return: This method does not return anything.
        """
        await self.destroy_injections()
        self.unwire_injections()

    def wire_injections(self, modules=None, *args, **kwargs) -> None:
        """Connect the configuration.

        :return: This method does not return anything.
        """
        from . import (
            decorators,
        )

        if modules is None:
            modules = tuple()
        modules = [*modules, decorators]

        self.container.wire(modules=modules, *args, **kwargs)

    def unwire_injections(self) -> None:
        """Disconnect the configuration.

        :return: This method does not return anything.
        """
        self.container.unwire()

    async def setup_injections(self) -> None:
        """Set Up the injections.

        :return: This method does not return anything.
        """
        await gather(*(injection.setup() for injection in self.injections.values()))

    async def destroy_injections(self) -> None:
        """Destroy the injections

        :return: This method does not return anything.
        """
        await gather(*(injection.destroy() for injection in self.injections.values()))

    @cached_property
    def container(self) -> containers.Container:
        """Get the dependencies' container.

        :return: A ``Container`` instance.
        """
        container = containers.DynamicContainer()
        container.config = providers.Object(self.config)

        for name, injection in self.injections.items():
            container.set_provider(name, providers.Object(injection))
        return container

    def __getattr__(self, item: str) -> InjectableMixin:
        if item not in self.injections:
            raise AttributeError(f"{type(self).__name__!r} does not contain the {item!r} attribute.")
        return self.injections[item]
