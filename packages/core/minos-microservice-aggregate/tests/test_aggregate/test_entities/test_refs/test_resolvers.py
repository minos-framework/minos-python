import unittest
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    Ref,
    RefResolver,
)
from minos.common import (
    ModelType,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
)
from tests.utils import (
    MinosTestCase,
)

Bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
Foo = ModelType.build("Foo", {"uuid": UUID, "version": int, "another": Ref[Bar]})


class TestRefResolver(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.resolver = RefResolver()

        self.uuid = uuid4()
        self.another_uuid = uuid4()
        self.value = Foo(self.uuid, 1, another=Ref(self.another_uuid))

    async def test_resolve(self):
        self.broker_subscriber_builder.with_messages(
            [BrokerMessageV1("", BrokerMessageV1Payload([Bar(self.value.another.uuid, 1)]))]
        )

        resolved = await self.resolver.resolve(self.value)

        observed = self.broker_publisher.messages

        self.assertEqual(1, len(observed))
        self.assertIsInstance(observed[0], BrokerMessageV1)
        self.assertEqual("GetBarSnapshots", observed[0].topic)
        self.assertEqual({"uuids": {self.another_uuid}}, observed[0].content)

        self.assertEqual(Foo(self.uuid, 1, another=Ref(Bar(self.another_uuid, 1))), resolved)

    async def test_resolve_already(self):
        self.assertEqual(34, await self.resolver.resolve(34))
        observed = self.broker_publisher.messages
        self.assertEqual(0, len(observed))


if __name__ == "__main__":
    unittest.main()
