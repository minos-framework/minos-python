from typing import (
    Any,
)

from minos.common import (
    BucketModel,
    ModelField,
    TypeHintBuilder,
)


class SagaContext(BucketModel):
    """Saga Context class

    The purpose of this class is to keep an execution state.
    """

    def __init__(self, **kwargs):
        if "fields" not in kwargs:
            kwargs["fields"] = {
                name: ModelField(name, TypeHintBuilder(value).build(), value) for name, value in kwargs.items()
            }
        super().__init__(**kwargs)

    def __setattr__(self, key: str, value: Any) -> None:
        try:
            super().__setattr__(key, value)
        except AttributeError:
            self._fields[key] = ModelField(key, TypeHintBuilder(value).build(), value)
