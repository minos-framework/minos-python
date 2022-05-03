from collections import (
    defaultdict,
)
from operator import (
    attrgetter,
)
from typing import (
    Any,
    Iterable,
    Optional,
    Union,
    get_args,
    get_origin,
)
from uuid import (
    UUID,
)

from minos.common import (
    TypeHintBuilder,
    is_model_type,
)

from .models import (
    Ref,
)


class RefExtractor:
    """Model Reference Extractor class."""

    def __init__(self, value: Any, type_: Optional[type] = None, as_uuids: bool = True):
        if type_ is None:
            type_ = TypeHintBuilder(value).build()
        self.value = value
        self.type_ = type_
        self.as_uuids = as_uuids

    def build(self) -> dict[str, set[UUID]]:
        """Run the model reference extractor.

        :return: A dictionary in which the keys are the class names and the values are the identifiers.
        """
        ans = defaultdict(set)
        self._build(self.value, self.type_, ans)

        if self.as_uuids:
            ans = {k: set(map(attrgetter("uuid"), v)) for k, v in ans.items()}

        return ans

    def _build(self, value: Any, type_: type, ans: dict[str, set[Ref]]) -> None:
        if get_origin(type_) is Union:
            type_ = next((t for t in get_args(type_) if get_origin(t) is Ref), type_)

        if isinstance(value, (tuple, list, set)):
            self._build_iterable(value, get_args(type_)[0], ans)

        elif isinstance(value, dict):
            self._build_iterable(value.keys(), get_args(type_)[0], ans)
            self._build_iterable(value.values(), get_args(type_)[1], ans)

        elif isinstance(value, Ref):
            cls = value.data_cls
            if cls is None and len(args := get_args(type_)):
                cls = args[0]
            if cls is None and len(args := get_args(type_.type_hints["data"])):
                cls = args[0]
            name = cls.__name__
            ans[name].add(value)

        elif is_model_type(value):
            # noinspection PyUnresolvedReferences
            for field in value.fields.values():
                self._build(field.value, field.type, ans)

    def _build_iterable(self, value: Iterable, value_: type, ans: dict[str, set[Ref]]) -> None:
        for sub_value in value:
            self._build(sub_value, value_, ans)
