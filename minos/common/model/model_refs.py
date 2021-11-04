from __future__ import (
    annotations,
)

from collections import (
    defaultdict,
)
from typing import (
    Any,
    Generic,
    Iterable,
    Optional,
    TypeVar,
    Union,
    get_args,
    get_origin,
)
from uuid import (
    UUID,
    SafeUUID,
)

from .abc import (
    Model,
)
from .declarative import (
    DeclarativeModel,
)
from .types import (
    TypeHintBuilder,
    is_model_type,
)

MT = TypeVar("MT")


class ModelRef(DeclarativeModel, UUID, Generic[MT]):
    """Model Reference."""

    data: Union[MT, UUID]

    def __init__(self, data: Union[MT, UUID], *args, **kwargs):
        if not isinstance(data, UUID) and not hasattr(data, "uuid"):
            raise ValueError()
        DeclarativeModel.__init__(self, data, *args, **kwargs)

    def __getattr__(self, item: str) -> Any:
        try:
            return super().__getattr__(item)
        except AttributeError as exc:
            if item != "data":
                return getattr(self.data, item)
            raise exc

    @property
    def int(self) -> int:
        """TODO"""
        return self.uuid.int

    @property
    def is_safe(self) -> SafeUUID:
        """TODO"""
        return self.uuid.is_safe

    def __eq__(self, other):
        return super().__eq__(other) or self.uuid == other or self.data == other

    def __hash__(self):
        return hash(self.uuid)

    @property
    def uuid(self) -> UUID:
        """TODO"""
        if isinstance(self.data, UUID):
            return self.data
        return self.data.uuid


class ModelRefExtractor:
    """Model Reference Extractor class."""

    def __init__(self, value: Any, type_: Optional[type] = None):
        if type_ is None:
            type_ = TypeHintBuilder(value).build()
        self.value = value
        self.type_ = type_

    def build(self) -> dict[str, set[ModelRef]]:
        """Run the model reference extractor.

        :return: A dictionary in which the keys are the class names and the values are the identifiers.
        """
        ans = defaultdict(set)
        self._build(self.value, self.type_, ans)
        return ans

    def _build(self, value: Any, type_: type, ans: dict[str, set[ModelRef]]) -> None:
        if get_origin(type_) is Union:
            type_ = next((t for t in get_args(type_) if get_origin(t) is ModelRef), type_)

        if isinstance(value, (tuple, list, set)):
            self._build_iterable(value, get_args(type_)[0], ans)

        elif isinstance(value, dict):
            self._build_iterable(value.keys(), get_args(type_)[0], ans)
            self._build_iterable(value.values(), get_args(type_)[1], ans)

        elif isinstance(value, UUID) and get_origin(type_) is ModelRef:
            cls = get_args(type_)[0]
            name = cls.__name__
            if not isinstance(value, ModelRef):
                value = type_(value)
            ans[name].add(value)

        elif is_model_type(value):
            for field in value.fields.values():
                self._build(field.value, field.type, ans)

    def _build_iterable(self, value: Iterable, value_: type, ans: dict[str, set[ModelRef]]) -> None:
        for sub_value in value:
            self._build(sub_value, value_, ans)


class ModelRefInjector:
    """Model Reference Injector class."""

    def __init__(self, value: Any, mapper: dict[UUID, Model]):
        self.value = value
        self.mapper = mapper

    def build(self) -> Any:
        """Inject the model instances referenced by identifiers.

        :return: A model in which the model references have been replaced by the values.
        """
        return self._build(self.value)

    def _build(self, value: Any) -> Any:
        if isinstance(value, (tuple, list, set)):
            return type(value)(self._build(v) for v in value)

        if isinstance(value, dict):
            return type(value)((self._build(k), self._build(v)) for k, v in value.items())

        if isinstance(value, UUID) and value in self.mapper:
            return self.mapper[value]

        if is_model_type(value):
            for field in value.fields.values():
                field.value = self._build(field.value)
            return value

        return value
