"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from base64 import (
    b64decode,
    b64encode,
)
from typing import (
    Any,
    Iterable,
    Union,
)

from ..exceptions import (
    EmptyMinosModelSequenceException,
    MultiTypeMinosModelSequenceException,
)
from ..protocol import (
    MinosAvroProtocol,
)
from .abc import (
    MinosModelAvroSchemaBuilder,
    ModelField,
)
from .aggregate import (
    Aggregate,
)

logger = logging.getLogger(__name__)


def _diff(a: dict, b: dict) -> dict:
    d = set(a.items()) - set(b.items())
    return dict(d)


class AggregateDiff(object):
    """TODO"""

    _fields: dict[str, ModelField]

    def __init__(self, fields: dict[str, ModelField]):
        self._fields = fields

    @classmethod
    def from_update(cls, old: Aggregate, new: Aggregate):
        """TODO

        :param new: TODO
        :param old: TODO
        :return: TODO
        """
        logger.debug(f"Computing the {cls!r} between {new!r} and {old!r}...")

        if new.id != old.id:
            raise Exception()  # TODO
        fields = _diff(new.fields, old.fields)

        fields.pop("version")

        return cls(fields)

    @classmethod
    def from_create(cls, aggregate: Aggregate):
        """TODO

        :param aggregate: TODO
        :return: TODO
        """
        fields = dict(aggregate.fields)
        fields.pop("id")
        fields.pop("version")

        return cls(fields)

    @classmethod
    def simplify(cls, *args: AggregateDiff):
        """TODO

        :param args: TODO
        :return: TODO
        """
        current = AggregateDiff(args[0].fields)
        for another in args[1:]:
            current._fields |= another._fields
        return current

    @classmethod
    def from_avro_str(cls, raw: str, **kwargs) -> Union[AggregateDiff, list[AggregateDiff]]:
        """Build a single instance or a sequence of instances from bytes

        :param raw: A bytes data.
        :return: A single instance or a sequence of instances.
        """
        raw = b64decode(raw.encode())
        return cls.from_avro_bytes(raw, **kwargs)

    @classmethod
    def from_avro_bytes(cls, raw: bytes, **kwargs) -> Union[AggregateDiff, list[AggregateDiff]]:
        """Build a single instance or a sequence of instances from bytes

        :param raw: A bytes data.
        :return: A single instance or a sequence of instances.
        """

        schema = MinosAvroProtocol.decode_schema(raw)
        decoded = MinosAvroProtocol.decode(raw)
        if isinstance(decoded, list):
            return [cls.from_avro(schema, d | kwargs) for d in decoded]
        return cls.from_avro(schema, decoded | kwargs)

    @classmethod
    def from_avro(cls, schema: dict[str, Any], data: dict[str, Any]) -> AggregateDiff:
        """TODO

        :param schema: TODO
        :param data: TODO
        :return: TODO
        """
        fields = dict()
        for raw in schema["fields"]:
            fields[raw["name"]] = ModelField.from_avro(raw, data[raw["name"]])
        return cls(fields)

    @classmethod
    def to_avro_str(cls, models: list[AggregateDiff]) -> str:
        """Create a bytes representation of the given object instances.

        :param models: A sequence of minos models.
        :return: A bytes object.
        """
        return b64encode(cls.to_avro_bytes(models)).decode()

    @classmethod
    def to_avro_bytes(cls, models: list[AggregateDiff]) -> bytes:
        """Create a bytes representation of the given object instances.

        :param models: A sequence of minos models.
        :return: A bytes object.
        """
        if len(models) == 0:
            raise EmptyMinosModelSequenceException("'models' parameter cannot be empty.")

        model_type = type(models[0])
        if not all(model_type == type(model) for model in models):
            raise MultiTypeMinosModelSequenceException(
                f"Every model must have type {model_type} to be valid. Found types: {[type(model) for model in models]}"
            )

        avro_schema = models[0].avro_schema
        # noinspection PyTypeChecker
        return MinosAvroProtocol().encode([model.avro_data for model in models], avro_schema)

    @property
    def avro_schema(self) -> list[dict[str, Any]]:
        """Compute the avro schema of the model.

        :return: A dictionary object.
        """

        fields = [MinosModelAvroSchemaBuilder(name, field.type).build() for name, field in self.fields.items()]
        # FIXME: The name must not be constant.
        return [{"name": type(self).__name__, "namespace": self.__module__, "type": "record", "fields": fields}]

    @property
    def avro_data(self) -> dict[str, Any]:
        """Compute the avro data of the model.

        :return: A dictionary object.
        """
        return {name: field.avro_data for name, field in self.fields.items()}

    @property
    def avro_str(self) -> str:
        """Generate bytes representation of the current instance.

        :return: A bytes object.
        """
        # noinspection PyTypeChecker
        return b64encode(self.avro_bytes).decode()

    @property
    def avro_bytes(self) -> bytes:
        """Generate bytes representation of the current instance.

        :return: A bytes object.
        """
        # noinspection PyTypeChecker
        return MinosAvroProtocol().encode(self.avro_data, self.avro_schema)

    def __eq__(self, other) -> bool:
        return type(self) == type(other) and self.fields == other.fields

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> Iterable:
        # noinspection PyRedundantParentheses
        yield from self.fields.items()

    def __repr__(self):
        fields_repr = ", ".join(repr(field) for field in self.fields.values())
        return f"{type(self).__name__}(fields=[{fields_repr}])"

    @property
    def fields(self) -> dict[str, ModelField]:
        """Fields getter"""
        return self._fields
