import dataclasses
import typing as t


class AggregateField:
    name: str
    type: t.Any
    value: t.Any

    def __init__(self, name: str, type: t.Any, value: t.Optional[t.Any] = None):
        self.name = name
        self.type = type
        self.value = value

    def __repr__(self):
        return f"AggregateField(name={self.name}, type={self.type}, value={self.value})"


def _process_aggregate(cls):
    # get the list of the fields from the __annotations__
    cls_annotations = cls.__dict__.get('__annotations__', {})
    aggregate_fields = []
    for name, type in cls_annotations.items():
        attribute = getattr(cls, name, None)
        aggregate_fields.append(AggregateField(name=name, type=type, value=attribute))
        value = None
    setattr(cls, "_FIELDS", aggregate_fields)
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
