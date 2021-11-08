from minos.aggregate import (
    ValueObject,
)


class Address(ValueObject):
    street: str
    number: int
    zip_code: int


class Location(ValueObject):
    street: str
