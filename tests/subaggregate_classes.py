"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    Optional,
)

from minos.common import (
    Aggregate,
    SubAggregate,
)


class CartItem(SubAggregate):
    """Aggregate ``Owner`` class for testing purposes."""

    name: str
    quantity: int


class Cart(Aggregate):
    """Aggregate ``Car`` class for testing purposes."""

    user: int
    items: list[CartItem]
