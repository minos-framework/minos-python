import unittest
from typing import (
    Any,
)
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    ModelRef,
    ModelRefResolver,
)
from minos.common import (
    ModelType,
)
from minos.networks import (
    BrokerMessageV1,
)
from tests.utils import (
    FakeAsyncIterator,
    MinosTestCase,
)

Bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
Foo = ModelType.build("Foo", {"uuid": UUID, "version": int, "another": ModelRef[Bar]})
FakeMessage = ModelType.build("FakeMessage", {"content": Any})


class TestModelRefResolver(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.resolver = ModelRefResolver()

        self.uuid = uuid4()
        self.another_uuid = uuid4()
        self.value = Foo(self.uuid, 1, another=ModelRef(self.another_uuid))

    async def test_resolve(self):
        with patch("minos.networks.BrokerClient.send") as send_mock:
            with patch("minos.networks.BrokerClient.receive_many") as receive_many:
                receive_many.return_value = FakeAsyncIterator([FakeMessage([Bar(self.value.another.uuid, 1)])])
                resolved = await self.resolver.resolve(self.value)

        self.assertIsInstance(send_mock.call_args_list[0].args[0], BrokerMessageV1)
        self.assertEqual("GetBars", send_mock.call_args_list[0].args[0].topic)
        self.assertEqual({"uuids": {self.another_uuid}}, send_mock.call_args_list[0].args[0].content)

        self.assertEqual(Foo(self.uuid, 1, another=ModelRef(Bar(self.another_uuid, 1))), resolved)

    async def test_resolve_already(self):
        with patch("minos.networks.BrokerClient.send") as send_mock:
            self.assertEqual(34, await self.resolver.resolve(34))
        self.assertEqual([], send_mock.call_args_list)


if __name__ == "__main__":
    unittest.main()
