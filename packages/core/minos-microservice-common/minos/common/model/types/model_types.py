from __future__ import (
    annotations,
)

from functools import (
    lru_cache,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    NamedTuple,
    Optional,
    Type,
    Union,
)

from ...exceptions import (
    MinosImportException,
)
from ...importlib import (
    import_module,
)
from .generics import (
    GenericTypeProjector,
)

if TYPE_CHECKING:
    from ..abc import (
        Model,
    )


class ModelType(type):
    """Model Type class."""

    name: str
    namespace: str
    type_hints: dict[str, Type]

    @classmethod
    def build(
        mcs, name_: str, type_hints_: Optional[dict[str, type]] = None, *, namespace_: Optional[str] = None, **kwargs
    ) -> ModelType:
        """Build a new ``ModelType`` instance.

        :param name_: Name of the new type.
        :param type_hints_: Type hints of the new type.
        :param namespace_: Namespace of the new type.
        :param kwargs: Type hints of the new type as named parameters.
        :return: A ``ModelType`` instance.
        """
        if type_hints_ is None:
            type_hints_ = tuple(kwargs.items())
        else:
            if len(kwargs):
                raise ValueError("Type hints can be passed in a dictionary or as named parameters, but not both.")
            type_hints_ = tuple(type_hints_.items())

        if namespace_ is None:
            try:
                namespace_, name_ = name_.rsplit(".", 1)
            except ValueError:
                namespace_ = str()

        # noinspection PyTypeChecker
        return mcs._build(name_, type_hints_, namespace_)

    @classmethod
    @lru_cache()
    def _build(mcs, name_: str, type_hints_: tuple[tuple[str, type], ...], namespace_: Optional[str]):
        return mcs(name_, tuple(), {"type_hints": dict(type_hints_), "namespace": namespace_})

    @classmethod
    def from_typed_dict(mcs, typed_dict) -> ModelType:
        """Build a new ``ModelType`` instance from a ``typing.TypedDict``.

        :param typed_dict: Typed dict to be used as base.
        :return: A ``ModelType`` instance.
        """
        return mcs.build(typed_dict.__name__, typed_dict.__annotations__)

    @staticmethod
    def from_model(model: Union[Model, type[Model]]) -> ModelType:
        """Build a new instance from model class.

        :param model: The model class.
        :return: A new ``ModelType`` instance.
        """
        from .builders import (
            TypeHintParser,
        )

        type_hints = GenericTypeProjector.from_model(model).build()
        type_hints = {k: TypeHintParser(v).build() for k, v in type_hints.items()}

        # noinspection PyTypeChecker
        return ModelType.build(name_=model.classname, type_hints_=type_hints)

    def __call__(cls, *args, **kwargs) -> Model:
        return cls.model_cls.from_model_type(cls, *args, **kwargs)

    @property
    def model_cls(cls) -> Type[Model]:
        """Get the model class if defined or ``DataTransferObject`` otherwise.

        :return: A model class.
        """
        try:
            # noinspection PyTypeChecker
            return import_module(cls.classname)
        except MinosImportException:
            from ..dynamic import (
                DataTransferObject,
            )

            return DataTransferObject

    @property
    def name(cls) -> str:
        """Get the type name.

        :return: A string object.
        """
        return cls.__name__

    @property
    def classname(cls) -> str:
        """Get the full class name.

        :return: An string object.
        """
        if not len(cls.namespace):
            return cls.name
        return f"{cls.namespace}.{cls.name}"

    def __le__(cls, other: Any) -> bool:
        from .comparators import (
            TypeHintComparator,
        )

        return type(cls).__eq__(cls, other) or (
            type(cls) == type(other)
            and cls.name == other.name
            and cls.namespace == other.namespace
            and set(cls.type_hints.keys()) <= set(other.type_hints.keys())
            and all(TypeHintComparator(v, other.type_hints[k]).match() for k, v in cls.type_hints.items())
        )

    def __lt__(cls, other: Any) -> bool:
        from .comparators import (
            TypeHintComparator,
        )

        return (
            type(cls) == type(other)
            and cls.name == other.name
            and cls.namespace == other.namespace
            and set(cls.type_hints.keys()) < set(other.type_hints.keys())
            and all(TypeHintComparator(v, other.type_hints[k]).match() for k, v in cls.type_hints.items())
        )

    def __ge__(cls, other: Any) -> bool:
        from .comparators import (
            TypeHintComparator,
        )

        return type(cls).__eq__(cls, other) or (
            type(cls) == type(other)
            and cls.name == other.name
            and cls.namespace == other.namespace
            and set(cls.type_hints.keys()) >= set(other.type_hints.keys())
            and all(TypeHintComparator(v, cls.type_hints[k]).match() for k, v in other.type_hints.items())
        )

    def __gt__(cls, other: Any) -> bool:
        from .comparators import (
            TypeHintComparator,
        )

        return (
            type(cls) == type(other)
            and cls.name == other.name
            and cls.namespace == other.namespace
            and set(cls.type_hints.keys()) > set(other.type_hints.keys())
            and all(TypeHintComparator(v, cls.type_hints[k]).match() for k, v in other.type_hints.items())
        )

    def __eq__(cls, other: Any) -> bool:
        conditions = (
            cls._equal_with_model_type,
            cls._equal_with_model,
            cls._equal_with_inherited_model,
            cls._equal_with_bucket_model,
        )
        # noinspection PyArgumentList
        return any(condition(other) for condition in conditions)

    def _equal_with_model_type(cls, other: ModelType) -> bool:
        from .comparators import (
            TypeHintComparator,
        )

        return (
            type(cls) == type(other)
            and cls.name == other.name
            and cls.namespace == other.namespace
            and set(cls.type_hints.keys()) == set(other.type_hints.keys())
            and all(TypeHintComparator(v, other.type_hints[k]).match() for k, v in cls.type_hints.items())
        )

    def _equal_with_model(cls, other: Any) -> bool:
        return hasattr(other, "model_type") and cls == ModelType.from_model(other)

    def _equal_with_inherited_model(cls, other: ModelType) -> bool:
        return (
            type(cls) == type(other) and cls.model_cls != other.model_cls and issubclass(cls.model_cls, other.model_cls)
        )

    def _equal_with_bucket_model(self, other: Any) -> bool:
        from ..dynamic import (
            BucketModel,
        )

        return (
            hasattr(other, "model_cls")
            and issubclass(self.model_cls, other.model_cls)
            and issubclass(other.model_cls, BucketModel)
        )

    def __hash__(cls) -> int:
        return hash(tuple(cls))

    def __iter__(cls) -> Iterable:
        # noinspection PyRedundantParentheses
        yield from (cls.name, cls.namespace, tuple(cls.type_hints.items()))

    def __repr__(cls):
        return f"{type(cls).__name__}(name={cls.name!r}, namespace={cls.namespace!r}, type_hints={cls.type_hints!r})"


class FieldType(NamedTuple):
    """Field Type class."""

    name: str
    type: type
