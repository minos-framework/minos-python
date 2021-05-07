"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from pathlib import (
    Path,
)

from minos.common import (
    MinosModel,
)
from minos.saga import (
    SagaContext,
)

BASE_PATH = Path(__file__).parent


class Foo(MinosModel):
    """TODO"""

    foo: str


# noinspection PyUnusedLocal
def foo_fn(context: SagaContext) -> MinosModel:
    """TODO

    :param context: TODO
    :return: TODO
    """
    return Foo("hello")
