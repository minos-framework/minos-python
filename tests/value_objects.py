from minos.common import (
    ValueObject,
)


class Address(ValueObject):
    street: str
    number: int
    zip_code: int


class Location(ValueObject):
    street: str
