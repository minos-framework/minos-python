import unittest
from typing import (
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    Ref,
    RefExtractor,
)
from minos.common import (
    ModelType,
)


# noinspection PyPep8Naming
class TestRefExtractor(unittest.TestCase):
    def test_simple(self):
        Foo = ModelType.build("Foo", {"uuid": UUID, "version": int})
        value = Ref(uuid4())

        expected = {Foo: {value}}
        observed = RefExtractor(value, Ref[Foo]).build()

        self.assertEqual(expected, observed)

    def test_list(self):
        Foo = ModelType.build("Foo", {"uuid": UUID, "version": int})
        value = [Ref(uuid4()), Ref(uuid4())]

        expected = {Foo: set(value)}
        observed = RefExtractor(value, list[Ref[Foo]]).build()

        self.assertEqual(expected, observed)

    def test_dict(self):
        Key = ModelType.build("Key", {"uuid": UUID, "version": int})
        Value = ModelType.build("Value", {"uuid": UUID, "version": int})
        value = {Ref(uuid4()): Ref(uuid4()), Ref(uuid4()): Ref(uuid4())}

        expected = {Key: set(value.keys()), Value: set(value.values())}
        observed = RefExtractor(value, dict[Ref[Key], Ref[Value]]).build()

        self.assertEqual(expected, observed)

    def test_model(self):
        Bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
        Foo = ModelType.build("Foo", {"uuid": UUID, "version": int, "another": Ref[Bar]})

        value = Foo(uuid=uuid4(), version=1, another=Ref(uuid4()))

        expected = {Bar: {value.another}}
        observed = RefExtractor(value, Foo).build()

        self.assertEqual(expected, observed)

    def test_model_without_kind(self):
        Bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
        Foo = ModelType.build("Foo", {"uuid": UUID, "version": int, "another": Ref[Bar]})

        value = Foo(uuid=uuid4(), version=1, another=Ref(uuid4()))

        expected = RefExtractor(value, Foo).build()
        observed = RefExtractor(value).build()

        self.assertEqual(expected, observed)

    def test_optional(self):
        Foo = ModelType.build("Foo", {"uuid": UUID, "version": int})
        value = Ref(uuid4())

        expected = {Foo: {value}}
        observed = RefExtractor(value, Optional[Ref[Foo]]).build()

        self.assertEqual(expected, observed)

    def test_model_cls_by_type_hints(self):
        Foo = ModelType.build("Foo", {"uuid": UUID, "version": int})

        value = Ref(Foo(uuid4(), 1))
        self.assertEqual(Foo, value.data_cls)

    def test_model_cls_none(self):
        value = Ref(uuid4())
        self.assertEqual(None, value.data_cls)


if __name__ == "__main__":
    unittest.main()
