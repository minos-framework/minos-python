from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    DataDecoder,
    DataEncoder,
    Model,
    ModelType,
    SchemaEncoder,
)


class BrokerMessage(ABC, Model):
    """Broker Message base class."""

    @property
    @abstractmethod
    def topic(self) -> str:
        """Get the topic of the message.

        :return: A ``str`` value.
        """

    @property
    @abstractmethod
    def identifier(self) -> UUID:
        """Get the identifier of the message.

        :return: An ``UUID`` instance.
        """

    @property
    def should_reply(self) -> bool:
        """

        :return:
        """
        return self.reply_topic is not None

    @abstractmethod
    def set_reply_topic(self, value: Optional[str]) -> None:
        """Set the message's reply topic.

        :param value: A ``str`` value or ``None``.
        :return: This method does not return anything.
        """

    @property
    @abstractmethod
    def reply_topic(self) -> Optional[str]:
        """Get the reply topic of the message if there is someone.

        :return: A ``str`` value or ``None``.
        """

    # noinspection PyPropertyDefinition
    @classmethod
    @property
    @abstractmethod
    def version(cls) -> int:
        """Get the version of the message.

        :return: A strictly positive ``int`` value.
        """

    @property
    @abstractmethod
    def content(self) -> Any:
        """Get the content of the message.

        :return: Any value.
        """

    @property
    @abstractmethod
    def ok(self) -> bool:
        """Check if the message is okay or not.

        :return: ``True`` if the message is okay or ``False`` otherwise.
        """

    @property
    @abstractmethod
    def status(self) -> int:
        """Get the status of the message.

        :return: An ``int`` instance.
        """

    @property
    @abstractmethod
    def headers(self) -> dict[str, str]:
        """Get the headers of the message.

        :return: A ``dict`` instance with ``str`` keys and ``str`` values.
        """

    # noinspection PyMethodParameters
    @classmethod
    def encode_schema(cls, encoder: SchemaEncoder, target: ModelType, **kwargs) -> Any:
        """Encode schema with the given encoder.

        :param encoder: The encoder instance.
        :param target: An optional pre-encoded schema.
        :return: The encoded schema of the instance.
        """
        if target.classname == BrokerMessage.classname:
            return super().encode_schema(encoder, target, **kwargs)

        # noinspection PyTypeChecker
        target = ModelType.build(BrokerMessage.classname, target.type_hints | {"version": int})
        return encoder.build(target, **kwargs)

    @classmethod
    def encode_data(cls, encoder: DataEncoder, target: Any, **kwargs) -> Any:
        """Encode data with the given encoder.

        :param encoder: The encoder instance.
        :param target: An optional pre-encoded data.
        :return: The encoded data of the instance.
        """
        encoded = encoder.build(target, **kwargs)
        encoded["version"] = cls.version
        return encoded

    @classmethod
    def decode_data(cls, decoder: DataDecoder, target: Any, type_: ModelType, **kwargs) -> BrokerMessage:
        """Decode data with the given decoder.

        :param decoder: The decoder instance.
        :param target: The data to be decoded.
        :param type_: The data type.
        :return: A decoded instance.
        """
        if type_.classname != BrokerMessage.classname:
            return super().decode_data(decoder, target, type_, **kwargs)

        # The versioning bifurcation must be done here.

        from .v1 import (
            BrokerMessageV1,
        )

        name = BrokerMessageV1.classname

        # noinspection PyTypeChecker
        type_ = ModelType.build(name, {n: t for n, t in type_.type_hints.items() if n != "version"})

        return decoder.build(target, type_, **kwargs)
