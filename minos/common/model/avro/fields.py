import abc
import collections
import dataclasses
import decimal
import inspect
import typing as t
import datetime
import uuid

import orjson

from minos.common.model.avro import utils, serialization
from minos.common.model import types
from minos.common.model.avro.model import MinosAvroModel


BOOLEAN = "boolean"
NULL = "null"
INT = "int"
FLOAT = "float"
LONG = "long"
DOUBLE = "double"
BYTES = "bytes"
STRING = "string"
ARRAY = "array"
ENUM = "enum"
MAP = "map"
FIXED = "fixed"
DATE = "date"
TIME_MILLIS = "time-millis"
TIMESTAMP_MILLIS = "timestamp-millis"
UUID = "uuid"
DECIMAL = "decimal"
LOGICAL_DATE = {"type": INT, "logicalType": DATE}
LOGICAL_TIME = {"type": INT, "logicalType": TIME_MILLIS}
LOGICAL_DATETIME = {"type": LONG, "logicalType": TIMESTAMP_MILLIS}
LOGICAL_UUID = {"type": STRING, "logicalType": UUID}

PYTHON_TYPE_TO_AVRO = {
    bool: BOOLEAN,
    type(None): NULL,
    int: LONG,
    float: DOUBLE,
    bytes: BYTES,
    str: STRING,
    list: {"type": ARRAY},
    tuple: {"type": ARRAY},
    dict: {"type": MAP},
    types.Fixed: {"type": FIXED},
    types.Enum: {"type": ENUM},
    datetime.date: {"type": INT, "logicalType": DATE},
    datetime.time: {"type": INT, "logicalType": TIME_MILLIS},
    datetime.datetime: {"type": LONG, "logicalType": TIMESTAMP_MILLIS},
    uuid.uuid4: {"type": STRING, "logicalType": UUID},
}

# excluding tuple because is a container
PYTHON_INMUTABLE_TYPES = (str, int, bool, float, bytes, type(None))

PYTHON_PRIMITIVE_CONTAINERS = (list, tuple, dict)

PYTHON_LOGICAL_TYPES = (datetime.date, datetime.time, datetime.datetime, uuid.uuid4, uuid.UUID, decimal.Decimal)

PYTHON_PRIMITIVE_TYPES = PYTHON_INMUTABLE_TYPES + PYTHON_PRIMITIVE_CONTAINERS

PRIMITIVE_AND_LOGICAL_TYPES = PYTHON_INMUTABLE_TYPES + PYTHON_LOGICAL_TYPES

PythonImnutableTypes = t.Union[
    str,
    int,
    bool,
    float,
    list,
    tuple,
    dict,
    datetime.date,
    datetime.time,
    datetime.datetime,
    uuid.UUID,
    decimal.Decimal,
]


@dataclasses.dataclass  # type: ignore
class BaseField:
    avro_type: t.ClassVar

    name: str
    type: t.Any  # store the python primitive type
    default: t.Any
    metadata: t.Mapping = dataclasses.field(default_factory=dict)

    @staticmethod
    def _get_self_reference_type(a_type: t.Any) -> str:
        internal_type = a_type.__args__[0]

        return internal_type.__forward_arg__

    def get_metadata(self) -> t.List[t.Tuple[str, str]]:
        meta_data_for_template = []
        try:
            metadata = dict(self.metadata)
            for name, value in metadata.items():
                meta_data_for_template.append((name, value))
        except (ValueError, TypeError):
            return meta_data_for_template
        return meta_data_for_template

    def render(self) -> collections.OrderedDict:
        """
        Render the fields base on the avro field
        At least will have name and type.
        returns:
            OrderedDict(
                ("name", "a name"),
                ("type", "a type"),
                ("default", "default value")
            )
            The default key is optional.
            If self.type is:
                * list, the OrderedDict will contains the key items inside type
                * tuple, he OrderedDict will contains the key symbols inside type
                * dict, he OrderedDict will contains the key values inside type
        """
        template = collections.OrderedDict([("name", self.name), ("type", self.get_avro_type())] + self.get_metadata())

        default = self.get_default_value()
        if default is not dataclasses.MISSING:
            template["default"] = default

        return template

    def get_default_value(self) -> t.Any:
        if self.default in (dataclasses.MISSING, None):
            return self.default
        else:
            self.validate_default()
            return self.default

    def validate_default(self) -> bool:
        msg = f"Invalid default type. Default should be {self.type}"
        assert isinstance(self.default, self.type), msg

        return True

    def to_json(self) -> str:
        return orjson.dumps(self.render(), indent=2)

    def to_dict(self) -> dict:
        return orjson.loads(self.to_json())

    @abc.abstractmethod
    def get_avro_type(self) -> t.Any:
        ...  # pragma: no cover


class InmutableField(BaseField):
    def get_avro_type(self) -> PythonImnutableTypes:
        if self.default is None:
            return [NULL, self.avro_type]
        return self.avro_type


@dataclasses.dataclass
class StringField(InmutableField):
    avro_type: t.ClassVar = STRING


@dataclasses.dataclass
class LongField(InmutableField):
    avro_type: t.ClassVar = LONG


@dataclasses.dataclass
class BooleanField(InmutableField):
    avro_type: t.ClassVar = BOOLEAN



@dataclasses.dataclass
class DoubleField(InmutableField):
    avro_type: t.ClassVar = DOUBLE


@dataclasses.dataclass
class BytesField(InmutableField):
    avro_type: t.ClassVar = BYTES

    def get_default_value(self) -> t.Any:
        if self.default in (dataclasses.MISSING, None):
            return self.default
        else:
            self.validate_default()
            return self.to_avro(self.default)

    @staticmethod
    def to_avro(item: bytes) -> str:
        return item.decode()

@dataclasses.dataclass
class NoneField(InmutableField):
    avro_type: t.ClassVar = NULL


@dataclasses.dataclass
class ContainerField(BaseField):
    default_factory: t.Optional[t.Callable] = None

    def get_avro_type(self) -> PythonImnutableTypes:
        avro_type = self.avro_type
        avro_type["name"] = self.name

        return avro_type


@dataclasses.dataclass
class ListField(ContainerField):
    items_type: t.Any = None
    internal_field: t.Any = None

    def __post_init__(self) -> None:
        self.generate_items_type()

    @property
    def avro_type(self) -> t.Dict:
        return {"type": ARRAY, "items": self.items_type}

    def get_default_value(self) -> t.Union[t.List, dataclasses._MISSING_TYPE]:
        if self.default is None:
            return []
        elif callable(self.default_factory):
            # expecting a callable
            default = self.default_factory()
            assert isinstance(default, list), f"List is required as default for field {self.name}"

            clean_items = []
            for item in default:
                item_type = type(item)
                if item_type in LOGICAL_CLASSES:
                    clean_item = LOGICAL_TYPES_FIELDS_CLASSES[item_type].to_avro(item)  # type: ignore
                else:
                    clean_item = item
                clean_items.append(clean_item)

            return clean_items
        return dataclasses.MISSING

    def generate_items_type(self) -> t.Any:
        # because avro can have only one type, we take the first one
        items_type = self.type.__args__[0]
        name = self.name

        if utils.is_union(items_type):
            self.items_type = UnionField(
                name, items_type, default=self.default, default_factory=self.default_factory
            ).get_avro_type()
        else:
            self.internal_field = AvroField(name, items_type)
            self.items_type = self.internal_field.get_avro_type()



@dataclasses.dataclass
class DictField(ContainerField):
    values_type: t.Any = None
    internal_field: t.Any = None

    def __post_init__(self) -> None:
        self.generate_values_type()

    @property
    def avro_type(self) -> t.Dict[str, t.Any]:
        return {"type": MAP, "values": self.values_type}

    def get_default_value(self) -> t.Union[t.Dict[str, t.Any], dataclasses._MISSING_TYPE]:
        if self.default is None:
            return {}
        elif callable(self.default_factory):
            # expeting a callable
            default = self.default_factory()
            assert isinstance(default, dict), f"Dict is required as default for field {self.name}"

            clean_items = {}
            for key, value in default.items():
                value_type = type(value)
                if value_type in LOGICAL_CLASSES:
                    clean_item = LOGICAL_TYPES_FIELDS_CLASSES[value_type].to_avro(value)  # type: ignore
                else:
                    clean_item = value
                clean_items[key] = clean_item

            return clean_items
        return dataclasses.MISSING

    def generate_values_type(self) -> t.Any:
        """
        Process t.Dict. Avro assumes that the key of a map is always a string,
        so we take the second argument to determine the value type
        """
        values_type = self.type.__args__[1]

        name = self.name
        self.internal_field = AvroField(name, values_type)
        self.values_type = self.internal_field.get_avro_type()


@dataclasses.dataclass
class UnionField(BaseField):
    default_factory: t.Optional[t.Callable] = None
    unions: t.List = dataclasses.field(default_factory=list)
    internal_fields: t.List = dataclasses.field(default_factory=list)

    def __post_init__(self) -> None:
        self.unions = self.generate_unions_type()

    def generate_unions_type(self) -> t.List:
        """
        Generate union.
        Arguments:
            elements (t.List): List of python types
            default (t.Any): Default value
            default factory (t.Calleable): Callable to get the default value for
                a list or dict type
        Returns:
            t.List: List of avro types
        """
        elements = self.type.__args__
        name = self.name

        unions: t.List = []

        # Place default at front of list
        default_type = None
        if self.default is None and self.default_factory is dataclasses.MISSING:
            unions.insert(0, NULL)
        elif type(self.default) is not dataclasses._MISSING_TYPE:
            default_type = type(self.default)
            default_field = AvroField(name, default_type)
            unions.append(default_field.get_avro_type())
            self.internal_fields.append(default_field)

        for element in elements:
            # create the field and get the avro type
            field = AvroField(name, element)

            if field.get_avro_type() not in unions:
                unions.append(field.get_avro_type())
                self.internal_fields.append(field)

        return unions

    def get_avro_type(self) -> t.List:
        return self.unions

    def get_default_value(self) -> t.Any:
        is_default_factory_callable = callable(self.default_factory)

        if self.default in (dataclasses.MISSING, None) and not is_default_factory_callable:
            return self.default
        elif is_default_factory_callable:
            # expeting a callable
            default = self.default_factory()  # type: ignore
            assert isinstance(default, (dict, list)), f"Dict or List is required as default for field {self.name}"

            return default
        elif type(self.default) in LOGICAL_CLASSES:
            return LOGICAL_TYPES_FIELDS_CLASSES[type(self.default)].to_avro(self.default)  # type: ignore
        return self.default

@dataclasses.dataclass
class FixedField(BaseField):
    def get_avro_type(self) -> t.Dict[str, t.Any]:
        avro_type = {
            "type": FIXED,
            "name": self.name,
            "size": int(self.default.size),
        }

        if self.default.namespace is not None:
            avro_type["namespace"] = self.default.namespace

        if self.default.aliases is not None:
            avro_type["aliases"] = self.default.aliases

        return avro_type

    def get_default_value(self) -> dataclasses._MISSING_TYPE:
        return dataclasses.MISSING


@dataclasses.dataclass
class EnumField(BaseField):
    def get_avro_type(self) -> t.Dict[str, t.Any]:
        avro_type = {
            "type": ENUM,
            "name": self.name,
            "symbols": self.default.symbols,
        }

        if self.default.namespace is not None:
            avro_type["namespace"] = self.default.namespace

        if self.default.aliases is not None:
            avro_type["aliases"] = self.default.aliases

        return avro_type

    def get_default_value(self) -> t.Union[str, dataclasses._MISSING_TYPE, None]:
        default = self.default.default

        if default == types.MissingSentinel:
            return dataclasses.MISSING
        elif default in (dataclasses.MISSING, None):
            return default
        else:
            # check that the default value is listed in symbols
            assert (
                default in self.default.symbols
            ), f"The default value should be on of {self.default.symbols}. Current is {default}"
            return default


@dataclasses.dataclass
class SelfReferenceField(BaseField):
    def get_avro_type(self) -> t.Union[t.List[str], str]:
        str_type = self._get_self_reference_type(self.type)

        if self.default is None:
            # means that default value is None
            return [NULL, str_type]
        return str_type

    def get_default_value(self) -> t.Union[dataclasses._MISSING_TYPE, None]:
        # Only check for None because self reference default value can be only None
        if self.default is None:
            return None
        return dataclasses.MISSING


class LogicalTypeField(InmutableField):
    def get_default_value(self) -> t.Union[None, str, int, float]:
        if self.default in (dataclasses.MISSING, None):
            return self.default
        else:
            self.validate_default()
            # Convert to datetime and get the amount of days
            return self.to_avro(self.default)

    @staticmethod
    def to_avro(value: t.Any) -> t.Union[int, float, str]:
        ...  # type: ignore  # pragma: no cover


@dataclasses.dataclass
class DateField(LogicalTypeField):
    """
    The date logical type represents a date within the calendar,
    with no reference to a particular time zone or time of day.
    A date logical type annotates an Avro int, where the int stores
    the number of days from the unix epoch, 1 January 1970 (ISO calendar).
    """

    avro_type: t.ClassVar = LOGICAL_DATE

    @staticmethod
    def to_avro(date: datetime.date) -> int:
        """
        Convert to datetime and get the amount of days
        from the unix epoch, 1 January 1970 (ISO calendar)
        for a given date
        Arguments:
            date (datetime.date)
        Returns:
            int
        """
        date_time = datetime.datetime.combine(date, datetime.datetime.min.time())
        ts = (date_time - utils.epoch_naive).total_seconds()

        return int(ts / (3600 * 24))


@dataclasses.dataclass
class TimeField(LogicalTypeField):
    """
    The time-millis logical type represents a time of day,
    with no reference to a particular calendar,
    time zone or date, with a precision of one millisecond.
    A time-millis logical type annotates an Avro int,
    where the int stores the number of milliseconds after midnight, 00:00:00.000.
    """

    avro_type: t.ClassVar = LOGICAL_TIME

    @staticmethod
    def to_avro(time: datetime.time) -> int:
        """
        Returns the number of milliseconds after midnight, 00:00:00.000
        for a given time object
        Arguments:
            time (datetime.time)
        Returns:
            int
        """
        hour, minutes, seconds, microseconds = (
            time.hour,
            time.minute,
            time.second,
            time.microsecond,
        )

        return int((((hour * 60 + minutes) * 60 + seconds) * 1000) + (microseconds / 1000))


@dataclasses.dataclass
class DatetimeField(LogicalTypeField):
    """
    The timestamp-millis logical type represents an instant on the global timeline,
    independent of a particular time zone or calendar, with a precision of one millisecond.
    A timestamp-millis logical type annotates an Avro long,
    where the long stores the number of milliseconds from the unix epoch,
    1 January 1970 00:00:00.000 UTC.
    """

    avro_type: t.ClassVar = LOGICAL_DATETIME

    @staticmethod
    def to_avro(date_time: datetime.datetime) -> float:
        """
        Returns the number of milliseconds from the unix epoch,
        1 January 1970 00:00:00.000 UTC for a given datetime
        Arguments:
            date_time (datetime.datetime)
        Returns:
            float
        """
        if date_time.tzinfo:
            ts = (date_time - utils.epoch).total_seconds()
        else:
            ts = (date_time - utils.epoch_naive).total_seconds()

        return ts * 1000


@dataclasses.dataclass
class UUIDField(LogicalTypeField):
    avro_type: t.ClassVar = LOGICAL_UUID

    def validate_default(self) -> bool:
        msg = f"Invalid default type. Default should be {str} or {uuid.UUID}"
        assert isinstance(self.default, (str, uuid.UUID)), msg

        return True

    @staticmethod
    def to_avro(uuid: uuid.UUID) -> str:
        return str(uuid)


@dataclasses.dataclass
class RecordField(BaseField):
    def get_avro_type(self) -> t.Union[t.List, t.Dict]:
        record_type = self.type.avro_schema_to_python()

        # when there is a nested record replace its name
        # to avoid name colisions
        record_name = self.type.__name__.lower()
        if record_name not in self.name:
            name = f"{self.name}_{record_name}_record"
        else:
            name = f"{self.name}_record"

        record_type["name"] = name

        if self.default is None:
            return [NULL, record_type]
        return record_type


@dataclasses.dataclass
class DecimalField(BaseField):

    precision: int = -1
    scale: int = 0

    def __post_init__(self) -> None:
        self.set_precision_scale()

    def set_precision_scale(self) -> None:
        if self.default != types.MissingSentinel:
            if isinstance(self.default, decimal.Decimal):
                sign, digits, scale = self.default.as_tuple()
                self.scale = scale * -1  # Make scale positive, as that's what Avro expects
                # decimal.Context has a precision property
                # BUT the precision property is independent of the number of digits stored in the Decimal instance
                # # # FROM THE DOCS HERE https://docs.python.org/3/library/decimal.html
                #  The context precision does not affect how many digits are stored.
                #  That is determined exclusively by the number of digits in value.
                #  For example, Decimal('3.00000') records all five zeros even if the context precision is only three.
                # # #
                # Avro is concerned with *what form the number takes* and not with *handling errors in the Python env*
                # so we take the number of digits stored in the decimal as Avro precision
                self.precision = len(digits)
            elif isinstance(self.default, types.Decimal):
                self.scale = self.default.scale
                self.precision = self.default.precision
            else:
                raise ValueError("decimal.Decimal default types must be either decimal.Decimal or types.Decimal")
        else:
            raise ValueError(
                "decimal.Decimal default types must be specified to provide precision and scale,"
                " and must be either decimal.Decimal or types.Decimal"
            )

        # Validation on precision and scale per Avro schema
        if self.precision <= 0:
            raise ValueError("Precision must be a positive integer greater than zero")

        if self.scale < 0 or self.precision < self.scale:
            raise ValueError("Scale must be zero or a positive integer less than or equal to the precision.")

            # Just pull the precision from default context and default out scale
            # Not ideal
            #
            # self.precision = decimal.Context().prec

    def get_avro_type(self) -> t.Dict[str, t.Any]:
        avro_type = {"type": BYTES, "logicalType": DECIMAL, "precision": self.precision, "scale": self.scale}

        return avro_type

    def get_default_value(self) -> t.Union[str, dataclasses._MISSING_TYPE, None]:
        default = self.default
        if isinstance(default, types.Decimal):
            default = default.default

        if default == types.MissingSentinel:
            return dataclasses.MISSING
        return serialization.decimal_to_str(default, self.precision, self.scale)


INMUTABLE_FIELDS_CLASSES = {
    bool: BooleanField,
    int: LongField,
    float: DoubleField,
    bytes: BytesField,
    str: StringField,
    type(None): NoneField,
}

CONTAINER_FIELDS_CLASSES = {
    tuple: ListField,
    list: ListField,
    collections.abc.Sequence: ListField,
    collections.abc.MutableSequence: ListField,
    dict: DictField,
    collections.abc.Mapping: DictField,
    collections.abc.MutableMapping: DictField,
    t.Union: UnionField,
}

LOGICAL_TYPES_FIELDS_CLASSES = {
    datetime.date: DateField,
    datetime.time: TimeField,
    datetime.datetime: DatetimeField,
    uuid.uuid4: UUIDField,
    uuid.UUID: UUIDField,
    bytes: BytesField,
    decimal.Decimal: DecimalField,
}

PRIMITIVE_LOGICAL_TYPES_FIELDS_CLASSES = {
    **INMUTABLE_FIELDS_CLASSES,
    **LOGICAL_TYPES_FIELDS_CLASSES,  # type: ignore
    types.Fixed: FixedField,
    types.Enum: EnumField,
}

LOGICAL_CLASSES = LOGICAL_TYPES_FIELDS_CLASSES.keys()

FieldType = t.Union[
    StringField,
    LongField,
    BooleanField,
    DoubleField,
    BytesField,
    NoneField,
    ListField,
    DictField,
    UnionField,
    FixedField,
    EnumField,
    SelfReferenceField,
    DateField,
    TimeField,
    DatetimeField,
    UUIDField,
    RecordField,
    InmutableField,
]


def field_factory(
    name: str,
    native_type: t.Any,
    default: t.Any = dataclasses.MISSING,
    default_factory: t.Any = dataclasses.MISSING,
    metadata: t.Mapping = dataclasses.field(default_factory=dict),
) -> FieldType:
    if native_type in PYTHON_INMUTABLE_TYPES:
        klass = INMUTABLE_FIELDS_CLASSES[native_type]
        return klass(name=name, type=native_type, default=default, metadata=metadata)
    elif utils.is_self_referenced(native_type):
        return SelfReferenceField(name=name, type=native_type, default=default, metadata=metadata)
    elif native_type is types.Fixed:
        return FixedField(name=name, type=native_type, default=default, metadata=metadata)
    elif native_type is types.Enum:
        return EnumField(name=name, type=native_type, default=default, metadata=metadata)
    elif isinstance(native_type, t._GenericAlias):  # type: ignore
        origin = native_type.__origin__

        if origin not in (
            tuple,
            list,
            dict,
            t.Union,
            collections.abc.Sequence,
            collections.abc.MutableSequence,
            collections.abc.Mapping,
            collections.abc.MutableMapping,
        ):
            raise ValueError(
                f"""
                Invalid Type for field {name}. Accepted types are list, tuple, dict or t.Union
                """
            )

        container_klass = CONTAINER_FIELDS_CLASSES[origin]
        return container_klass(  # type: ignore
            name=name,
            type=native_type,
            default=default,
            metadata=metadata,
            default_factory=default_factory,
        )
    elif native_type in PYTHON_LOGICAL_TYPES:
        klass = LOGICAL_TYPES_FIELDS_CLASSES[native_type]  # type: ignore
        return klass(name=name, type=native_type, default=default, metadata=metadata)
    elif inspect.isclass(native_type) and issubclass(native_type, MinosAvroModel):
        return RecordField(name=name, type=native_type, default=default, metadata=metadata)
    else:
        msg = (
            f"Type {native_type} is unknown."
        )

        raise ValueError(msg)


AvroField = field_factory
