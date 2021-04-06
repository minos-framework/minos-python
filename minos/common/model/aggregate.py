import dataclasses
from collections import namedtuple

from minos.common.logs import log

AVRO_FIELD = namedtuple("MinosAvroField", "name type")


def _process_aggregate(cls):
    # get the list of the fields from the __annotations__
    cls_annotations = cls.__dict__.get('__annotations__', {})
    log.debug("Testing")
    for name, type in cls_annotations.items():

        log.debug(f"Annotations: {name}, {type}")

    return cls


def aggregate(cls=None):
    def wrap(cls):
        return _process_aggregate(cls)

    if cls is None:
        return wrap

    return wrap(cls)


class MinosBaseAggregate:

    @property
    def schema(self):
        """
        this property return the avro schema for the aggregate
        """

        # chekck if the derived class is a dataclass, if not create one
        dataclass_instance = self._get_dataclass()
        fields = [
            AVRO_FIELD(
                name=dataclass_field.name,
                type=dataclass_field.type
            )
            for dataclass_field in dataclasses.fields(self)
        ]
        return fields

    def _get_dataclass(self):
        if dataclasses.is_dataclass(self):
            return self
        return dataclasses.dataclass(self)
