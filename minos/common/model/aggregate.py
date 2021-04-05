import dataclasses
from collections import namedtuple

AVRO_FIELD = namedtuple("MinosAvroField", "name type")


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
