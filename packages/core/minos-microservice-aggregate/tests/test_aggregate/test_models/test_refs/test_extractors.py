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


class TestRefExtractor(unittest.TestCase):
    def test_simple(self):
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int})
        value = Ref(uuid4())

        expected = {"Foo": {value}}
        observed = RefExtractor(value, Ref[mt_foo]).build()

        self.assertEqual(expected, observed)

    def test_list(self):
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int})
        value = [Ref(uuid4()), Ref(uuid4())]

        expected = {"Foo": set(value)}
        observed = RefExtractor(value, list[Ref[mt_foo]]).build()

        self.assertEqual(expected, observed)

    def test_dict(self):
        mt_key = ModelType.build("Key", {"uuid": UUID, "version": int})
        mt_value = ModelType.build("Value", {"uuid": UUID, "version": int})
        value = {Ref(uuid4()): Ref(uuid4()), Ref(uuid4()): Ref(uuid4())}

        expected = {"Key": set(value.keys()), "Value": set(value.values())}
        observed = RefExtractor(value, dict[Ref[mt_key], Ref[mt_value]]).build()

        self.assertEqual(expected, observed)

    def test_model(self):
        mt_bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int, "another": Ref[mt_bar]})

        value = mt_foo(uuid=uuid4(), version=1, another=Ref(uuid4()))

        expected = {"Bar": {value.another}}
        observed = RefExtractor(value, mt_foo).build()

        self.assertEqual(expected, observed)

    def test_model_without_kind(self):
        mt_bar = ModelType.build("Bar", {"uuid": UUID, "version": int})
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int, "another": Ref[mt_bar]})

        value = mt_foo(uuid=uuid4(), version=1, another=Ref(uuid4()))

        expected = RefExtractor(value, mt_foo).build()
        observed = RefExtractor(value).build()

        self.assertEqual(expected, observed)

    def test_optional(self):
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int})
        value = Ref(uuid4())

        expected = {"Foo": {value}}
        observed = RefExtractor(value, Optional[Ref[mt_foo]]).build()

        self.assertEqual(expected, observed)

    def test_model_cls_by_type_hints(self):
        mt_foo = ModelType.build("Foo", {"uuid": UUID, "version": int})

        value = Ref(mt_foo(uuid4(), 1))
        self.assertEqual(mt_foo, value.data_cls)

    def test_model_cls_none(self):
        value = Ref(uuid4())
        self.assertEqual(None, value.data_cls)


if __name__ == "__main__":
    unittest.main()
