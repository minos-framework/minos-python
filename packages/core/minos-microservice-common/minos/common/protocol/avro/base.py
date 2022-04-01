import io
from typing import (
    Any,
    Union,
)

from fastavro import (
    parse_schema,
    reader,
    writer,
)

from ...exceptions import (
    MinosProtocolException,
)
from ..abc import (
    MinosBinaryProtocol,
)


class MinosAvroProtocol(MinosBinaryProtocol):
    """Minos Avro Protocol class."""

    @classmethod
    def encode(cls, value: Any, schema: Any, *args, batch_mode: bool = False, **kwargs) -> bytes:
        """Encoder in avro for database Values
        all the headers are converted in fields with double underscore name
        the body is a set fields coming from the data type.

        :param value: The data to be stored.
        :param schema: The schema relative to the data.
        :param args: Additional positional arguments.
        :param batch_mode: If ``True`` the data is processed as a list of models, otherwise the data is processed as a
            single model.
        :param kwargs: Additional named arguments.
        :return: A bytes object.
        """
        if not batch_mode:
            value = [value]

        if not isinstance(schema, list):
            schema = [schema]

        try:
            raw_schema = cls._parse_schema(schema)
            return cls._write_data(value, raw_schema)
        except Exception as exc:
            raise MinosProtocolException(f"Error encoding data: {exc!r}")

    @staticmethod
    def _parse_schema(schema: list[dict[str, Any]]) -> dict[str, Any]:
        named_schemas = {}
        for item in schema[1::-1]:
            parse_schema(item, named_schemas)
        return parse_schema(schema[0], named_schemas, expand=True)

    @staticmethod
    def _write_data(value: list[dict[str, Any]], schema: dict[str, Any]):
        with io.BytesIO() as file:
            writer(file, schema, value)
            file.seek(0)
            content = file.getvalue()
        return content

    @classmethod
    def decode(cls, data: bytes, *args, batch_mode: bool = False, **kwargs) -> Any:
        """Decode the given bytes of data into a single dictionary or a sequence of dictionaries.

        :param data: A bytes object.
        :param args: Additional positional arguments.
        :param batch_mode: If ``True`` the data is processed as a list of models, otherwise the data is processed as a
            single model.
        :param kwargs: Additional named arguments.
        :return: A dictionary or a list of dictionaries.
        """

        try:
            with io.BytesIO(data) as file:
                ans = list(reader(file))
        except Exception as exc:
            raise MinosProtocolException(f"Error decoding the avro bytes: {exc}")

        if not batch_mode:
            if len(ans) > 1:
                raise MinosProtocolException(
                    f"The 'batch_mode' argument was set to {False!r} but data contains multiple values: {ans!r}"
                )
            ans = ans[0]

        return ans

    # noinspection PyUnusedLocal
    @classmethod
    def decode_schema(cls, data: bytes, *args, **kwargs) -> Union[dict[str, Any], list[dict[str, Any]]]:
        """Decode the given bytes of data into a single dictionary or a sequence of dictionaries.

        :param data: A bytes object.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A tuple or a list of tuples.
        """

        try:
            with io.BytesIO(data) as file:
                r = reader(file)
                schema = r.writer_schema

        except Exception as exc:
            raise MinosProtocolException(f"Error getting avro schema: {exc}")

        return schema
