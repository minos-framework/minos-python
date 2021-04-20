"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import Optional

from minos.common import (
    Aggregate,
    ModelRef,
)


class Owner(Aggregate):
    """Aggregate ``Owner`` class for testing purposes."""

    name: str
    surname: str
    age: Optional[int]


class Car(Aggregate):
    """Aggregate ``Car`` class for testing purposes."""

    doors: int
    color: str
    # owner: Optional[ModelRef[Owner]]  # FIXME: There is a bug on the schema generation.
