import unittest
from collections.abc import (
    Mapping,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    DeclarativeModel,
    Field,
    Model,
)
from tests.model_classes import (
    FooBar,
)


class TestModel(unittest.TestCase):
    def test_base(self):
        self.assertTrue(issubclass(Model, Mapping))

    def test_fields(self):
        uuid = uuid4()
        model = FooBar(uuid)

        self.assertEqual({"identifier": Field("identifier", UUID, uuid)}, model.fields)

    def test_eq_reversing(self):
        class _Fake(DeclarativeModel):
            def __eq__(self, other):
                return True

        self.assertEqual(FooBar(uuid4()), _Fake())
        self.assertEqual(_Fake(), FooBar(uuid4()))


if __name__ == "__main__":
    unittest.main()
