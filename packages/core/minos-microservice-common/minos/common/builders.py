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
    Union,
    get_args,
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

    def __init__(self, instance_cls: Optional[type[Instance]] = None, *args, **kwargs):

        super().__init__(*args, **kwargs)
        if instance_cls is None:
            instance_cls = self._get_cls()

        self.kwargs = dict()
        self.instance_cls = instance_cls

    def _get_cls(self) -> Optional[type]:
        # noinspection PyUnresolvedReferences
        bases = self.__orig_bases__

        instance_cls = get_args(next((base for base in bases if len(get_args(base))), None))[0]

        if not isinstance(instance_cls, type):
            return None

        return instance_cls

    def copy(self: type[B]) -> B:
        """Get a copy of the instance.

        :return: A ``Builder`` instance.
        """
        return self.new().with_cls(self.instance_cls).with_kwargs(self.kwargs)

    @classmethod
    def new(cls: type[B]) -> B:
        """Get a new instance.

        :return: A ``Builder`` instance.
        """
        return cls()

    def with_cls(self: B, cls: type) -> B:
        """Set class to be built.

        :param cls: The class to be set.
        :return: This method return the builder instance.
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

        :return: A ``Instance`` instance.
        """
        return self.instance_cls(**self.kwargs)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self.instance_cls == other.instance_cls and self.kwargs == other.kwargs

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.instance_cls.__name__}, {self.kwargs!r})"


Ins = TypeVar("Ins", bound="BuildableMixin")


class BuildableMixin(SetupMixin):
    """Buildable Mixin class."""

    _builder: Union[Builder[Ins], type[Builder[Ins]]] = Builder

    @classmethod
    def _from_config(cls, config: Config, **kwargs):
        return cls.get_builder().with_config(config).with_kwargs(kwargs).build()

    @classmethod
    def set_builder(cls: type[Ins], builder: Union[Builder[Ins], type[Builder[Ins]]]) -> None:
        """Set a builder class.

        :param builder: The builder class to be set.
        :return: This method does not return anything.
        """
        if not isinstance(builder, Builder) and not (isinstance(builder, type) and issubclass(builder, Builder)):
            raise ValueError(f"Given builder value is invalid: {builder!r}")

        cls._builder = builder

    @classmethod
    def get_builder(cls) -> Builder[Ins]:
        """Get the builder class.

        :return: A ``Builder`` instance.
        """
        builder = cls._builder

        if isinstance(builder, Builder):
            builder = builder.copy()
        else:
            builder = builder.new()

        return builder.with_cls(cls)


B = TypeVar("B", bound=Builder)
