from minos.common.model.declarative import (
    ValueObject,
)


class Address(ValueObject):
    street: str
    number: int
    zip_code: int
