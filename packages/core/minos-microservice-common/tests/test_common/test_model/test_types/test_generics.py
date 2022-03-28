import unittest
from typing import (
    Optional,
    TypeVar,
    Union,
)

from minos.common import (
    GenericTypeProjector,
)
from tests.model_classes import (
    GenericUser,
    T,
    User,
)

K = TypeVar("K", int, float)


class TestGenericTypeProjector(unittest.TestCase):
    def test_from_model(self):
        model = GenericUser
        projector = GenericTypeProjector.from_model(model)

        self.assertEqual({"username": T}, projector.type_hints)
        self.assertEqual(dict(), projector.mapper)

    def test_from_model_typed(self):
        model = GenericUser[str]
        projector = GenericTypeProjector.from_model(model)

        self.assertEqual({"username": T}, projector.type_hints)
        self.assertEqual({T: str}, projector.mapper)

    def test_from_model_typed_2(self):
        model = GenericUser("foo")
        projector = GenericTypeProjector.from_model(model)

        self.assertEqual({"username": str}, projector.type_hints)
        self.assertEqual(dict(), projector.mapper)

    def test_from_model_without_generics(self):
        model = User
        projector = GenericTypeProjector.from_model(model)

        self.assertEqual({"id": int, "username": Optional[str]}, projector.type_hints)
        self.assertEqual(dict(), projector.mapper)

    def test_from_model_without_generics_instance(self):
        model = User(1234, "johndoe")
        projector = GenericTypeProjector.from_model(model)

        self.assertEqual({"id": int, "username": Optional[str]}, projector.type_hints)
        self.assertEqual(dict(), projector.mapper)

    def test_build(self):
        projector = GenericTypeProjector({"id": int, "number": K}, {K: float})
        self.assertEqual({"id": int, "number": float}, projector.build())

    def test_build_without_mapping(self):
        projector = GenericTypeProjector({"id": int, "number": K}, dict())
        self.assertEqual({"id": int, "number": Union[int, float]}, projector.build())

    def test_build_without_coincidence(self):
        projector = GenericTypeProjector({"id": int, "number": int}, {K: float})
        self.assertEqual({"id": int, "number": int}, projector.build())


if __name__ == "__main__":
    unittest.main()
