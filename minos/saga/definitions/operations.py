"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    Any,
    Callable,
    Iterable,
    Optional,
    TypeVar,
    Union,
)

from minos.common import (
    classname,
    import_module,
)

from ..context import (
    SagaContext,
)

T = TypeVar("T")


def identity_fn(x: T) -> T:
    """A identity function, that returns the same value without any transformation.

    :param x: The input value.
    :return: This function return the input value without any transformation.
    """
    return x


class SagaOperation(object):
    """Saga Step Operation class."""

    def __init__(self, callback: Callable, name: Optional[str] = None, parameters: Optional[SagaContext] = None):
        self.callback = callback
        self.name = name
        self.parameters = parameters

    @property
    def raw(self) -> dict[str, Any]:
        """Generate a rew representation of the instance.

        :return: A ``dict`` instance.
        """
        # noinspection PyTypeChecker
        raw = {"callback": classname(self.callback)}

        if self.name is not None:
            raw["name"] = self.name

        if self.parameterized:
            raw["parameters"] = self.parameters.avro_str

        return raw

    @property
    def parameterized(self) -> bool:
        """parameterized getter.

        :return: ``True`` if parameters are provided or ``False`` otherwise.
        """
        return self.parameters is not None

    @classmethod
    def from_raw(cls, raw: Optional[Union[dict[str, Any], SagaOperation]], **kwargs) -> Optional[SagaOperation]:
        """Build a new instance from a raw representation.

        :param raw: A raw representation.
        :param kwargs: Additional named arguments.
        :return: A ``SagaStepOperation`` instance if the ``raw`` argument is not ``None``, ``None`` otherwise.
        """
        if raw is None:
            return None

        if isinstance(raw, cls):
            return raw

        current = raw | kwargs
        if isinstance(current["callback"], str):
            current["callback"] = import_module(current["callback"])

        if "parameters" in current:
            current["parameters"] = SagaContext.from_avro_str(current["parameters"])
        return cls(**current)

    def __eq__(self, other: SagaOperation) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.name,
            self.callback,
            self.parameters,
        )
