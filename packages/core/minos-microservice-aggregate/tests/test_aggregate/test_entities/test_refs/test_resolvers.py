import unittest
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    Ref,
    RefException,
    RefResolver,
)
from minos.common import (
    ModelType,
    NotProvidedException,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerMessageV1Status,
)
from tests.utils import (
    AggregateTestCase,
)

Bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
Foo = ModelType.build("Foo", {"uuid": UUID, "version": int, "another": Ref[Bar]})


class TestRefResolver(AggregateTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.resolver = RefResolver()

        self.uuid = uuid4()
        self.another_uuid = uuid4()
        self.value = Foo(self.uuid, 1, another=Ref(self.another_uuid))

    def test_broker_pool_not_provided(self):
        with self.assertRaises(NotProvidedException):
            # noinspection PyArgumentEqualDefault
            RefResolver(broker_pool=None, pool_factory=None)

    async def test_resolve(self):
        self.broker_subscriber_builder.with_messages(
            [BrokerMessageV1("", BrokerMessageV1Payload([Bar(self.value.another.uuid, 1)]))]
        )

        resolved = await self.resolver.resolve(self.value)

        observed = self.broker_publisher.messages

        self.assertEqual(1, len(observed))
        self.assertIsInstance(observed[0], BrokerMessageV1)
        self.assertEqual("_GetBarSnapshots", observed[0].topic)
        self.assertEqual({"uuids": {self.another_uuid}}, observed[0].content)

        self.assertEqual(Foo(self.uuid, 1, another=Ref(Bar(self.another_uuid, 1))), resolved)

    async def test_resolve_already(self):
        self.assertEqual(34, await self.resolver.resolve(34))
        observed = self.broker_publisher.messages
        self.assertEqual(0, len(observed))

    async def test_resolve_raises(self):
        self.broker_subscriber_builder.with_messages(
            [BrokerMessageV1("", BrokerMessageV1Payload(status=BrokerMessageV1Status.ERROR))]
        )
        with self.assertRaises(RefException):
            await self.resolver.resolve(self.value)

    def test_build_topic_name_str(self):
        expected = "_GetBarSnapshots"
        observed = RefResolver.build_topic_name("Bar")

        self.assertEqual(observed, expected)

    def test_build_topic_name_type(self):
        expected = "_GetBarSnapshots"
        observed = RefResolver.build_topic_name(Bar)

        self.assertEqual(observed, expected)


if __name__ == "__main__":
    unittest.main()
