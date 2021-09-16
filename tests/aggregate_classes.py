from __future__ import (
    annotations,
)

from typing import (
    Optional,
)

from minos.common import (
    Aggregate,
    Entity,
    EntitySet,
    ModelRef,
    ValueObject,
    ValueObjectSet,
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
    owner: Optional[list[ModelRef[Owner]]]


class Order(Aggregate):
    """For testing purposes"""

    products: EntitySet[OrderItem]
    reviews: ValueObjectSet[Review]


class OrderItem(Entity):
    """For testing purposes"""

    amount: int


class Review(ValueObject):
    """For testing purposes."""

    message: str
