from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    Generic,
    TypeVar,
)

from .config import (
    Config,
)
from .setup import (
    SetupMixin,
)

Instance = TypeVar("Instance")


class Builder(SetupMixin, ABC, Generic[Instance]):
    """Builder class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kwargs = dict()

    def copy(self: type[B]) -> B:
        """Get a copy of the instance.

        :return: A ``BrokerSubscriberBuilder`` instance.
        """
        return self.new().with_kwargs(self.kwargs)

    @classmethod
    def new(cls: type[B]) -> B:
        """Get a new instance.

        :return: A ``BrokerSubscriberBuilder`` instance.
        """
        return cls()

    def with_kwargs(self: B, kwargs: dict[str, Any]) -> B:
        """Set kwargs.

        :param kwargs: The kwargs to be set.
        :return: This method return the builder instance.
        """
        self.kwargs |= kwargs
        return self

    # noinspection PyUnusedLocal
    def with_config(self: B, config: Config) -> B:
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        return self

    @abstractmethod
    def build(self) -> Instance:
        """Build the instance.

        :return: A ``BrokerSubscriber`` instance.
        """


Ins = TypeVar("Ins", bound="BuildableMixin")


class BuildableMixin(SetupMixin):
    """Buildable Mixin class."""

    _builder_cls: type[Builder[Ins]]

    @classmethod
    def _from_config(cls: type[Ins], config: Config, **kwargs) -> Ins:
        return cls.get_builder().new().with_config(config).with_kwargs(kwargs).build()

    @classmethod
    def set_builder(cls: type[Ins], builder: type[Builder[Ins]]) -> None:
        """Set a builder class.

        :param builder: The builder class to be set.
        :return: This method does not return anything.
        """
        cls._builder_cls = builder

    @classmethod
    def get_builder(cls) -> type[Builder[Ins]]:
        """Get the builder class.

        :return: A ``Builder`` subclass.
        """
        return cls._builder_cls


B = TypeVar("B", bound=Builder)
