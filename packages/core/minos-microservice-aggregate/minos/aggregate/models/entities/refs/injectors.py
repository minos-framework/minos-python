from typing import (
    Any,
)
from uuid import (
    UUID,
)

from minos.common import (
    Model,
    is_model_type,
)


class RefInjector:
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
