from typing import (
    Any,
)

from minos.common import (
    BucketModel,
    Field,
)


class SagaContext(BucketModel):
    """Saga Context class

    The purpose of this class is to keep an execution state.
    """

    def __init__(self, **kwargs):
        if "fields" not in kwargs:
            fields = {name: Field(name, Any, value) for name, value in kwargs.items()}
        else:
            fields = {name: Field(field.name, Any, field.value) for name, field in kwargs["fields"].items()}

        super().__init__(fields=fields)

    def __setattr__(self, key: str, value: Any) -> None:
        try:
            super().__setattr__(key, value)
        except AttributeError:
            self._fields[key] = Field(key, Any, value)
