import unittest
from typing import (
    Any,
)
from unittest.mock import (
    call,
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
from tests.utils import (
    MinosTestCase,
)

Bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
Foo = ModelType.build("Foo", {"uuid": UUID, "version": int, "another": ModelRef[Bar]})
FakeEntry = ModelType.build("FakeEntry", {"data": Any})
FakeMessage = ModelType.build("FakeMessage", {"data": Any})


class TestModelRefResolver(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.resolver = ModelRefResolver()

        self.uuid = uuid4()
        self.another_uuid = uuid4()
        self.value = Foo(self.uuid, 1, another=ModelRef(self.another_uuid))

    async def test_resolve(self):
        with patch("tests.utils.FakeBroker.send") as send_mock:
            with patch("tests.utils.FakeBroker.get_many") as get_many:
                get_many.return_value = [FakeEntry(FakeMessage([Bar(self.value.another.uuid, 1)]))]
                resolved = await self.resolver.resolve(self.value)

        self.assertEqual([call(data={"uuids": {self.another_uuid}}, topic="GetBars")], send_mock.call_args_list)
        self.assertEqual(Foo(self.uuid, 1, another=ModelRef(Bar(self.another_uuid, 1))), resolved)

    async def test_resolve_already(self):
        with patch("tests.utils.FakeBroker.send") as send_mock:
            self.assertEqual(34, await self.resolver.resolve(34))
        self.assertEqual([], send_mock.call_args_list)


if __name__ == "__main__":
    unittest.main()
