from __future__ import (
    annotations,
)

from abc import (
    ABC,
)
from typing import (
    Any,
    Generic,
    Optional,
    TypeVar,
    get_args,
)

from .config import (
    Config,
)
from .setup import (
    SetupMixin,
)

Instance = TypeVar("Instance", bound=type)


class Builder(SetupMixin, ABC, Generic[Instance]):
    """Builder class."""

    def __init__(self, instance_cls: Optional[type[Instance]] = None, *args, **kwargs):

        super().__init__(*args, **kwargs)
        if instance_cls is None:
            instance_cls = self._get_cls()

        self.kwargs = dict()
        self.instance_cls = instance_cls

    def _get_cls(self) -> Optional[type]:
        # noinspection PyUnresolvedReferences
        bases = self.__orig_bases__

        try:
            instance_cls = get_args(next((base for base in bases if len(get_args(base))), None))[0]
        except Exception:
            return None

        if not isinstance(instance_cls, type):
            return None

        return instance_cls

    def copy(self: type[B]) -> B:
        """Get a copy of the instance.

        :return: A ``BrokerSubscriberBuilder`` instance.
        """
        return self.new().with_cls(self.instance_cls).with_kwargs(self.kwargs)

    @classmethod
    def new(cls: type[B]) -> B:
        """Get a new instance.

        :return: A ``BrokerSubscriberBuilder`` instance.
        """
        return cls()

    def with_cls(self: B, cls: type) -> B:
        """TODO

        :param cls: TODO
        :return: TODO
        """
        self.instance_cls = cls
        return self

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

    def build(self) -> Instance:
        """Build the instance.

        :return: A ``BrokerSubscriber`` instance.
        """
        return self.instance_cls(**self.kwargs)


Ins = TypeVar("Ins", bound="BuildableMixin")


class BuildableMixin(SetupMixin):
    """Buildable Mixin class."""

    _builder_cls: type[Builder[Ins]] = Builder

    @classmethod
    def _from_config(cls, config: Config, **kwargs):
        return cls.get_builder().new().with_cls(cls).with_config(config).with_kwargs(kwargs).build()

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
