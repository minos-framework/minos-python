import unittest
from datetime import (
    date,
    datetime,
    time,
    timedelta,
)
from enum import (
    Enum,
)
from typing import (
    Union,
)
from uuid import (
    UUID,
)

from graphql import (
    GraphQLBoolean,
    GraphQLFloat,
    GraphQLID,
    GraphQLInt,
    GraphQLString,
    is_enum_type,
    is_list_type,
    is_non_null_type,
    is_object_type,
)

from minos.common import (
    DeclarativeModel,
    ModelType,
    NoneType,
)
from minos.plugins.graphql import (
    GraphQlSchemaEncoder,
)


class TestGraphQlSchemaEncoder(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.encoder = GraphQlSchemaEncoder()

    def test_union(self):
        observed = self.encoder.build(Union[int, str])

        self.assertEqual(2, len(observed))

        first_observed = observed[0]
        self.assertTrue(is_non_null_type(first_observed))
        self.assertEqual(GraphQLInt, first_observed.of_type)

        second_observed = observed[1]
        self.assertTrue(is_non_null_type(second_observed))
        self.assertEqual(GraphQLString, second_observed.of_type)

    def test_none(self):
        observed = self.encoder.build(NoneType)

        self.assertEqual(GraphQLBoolean, observed)

    def test_bool(self):
        observed = self.encoder.build(bool)

        self.assertTrue(is_non_null_type(observed))
        self.assertEqual(GraphQLBoolean, observed.of_type)

    def test_int(self):
        observed = self.encoder.build(int)

        self.assertTrue(is_non_null_type(observed))
        self.assertEqual(GraphQLInt, observed.of_type)

    def test_float(self):
        observed = self.encoder.build(float)

        self.assertTrue(is_non_null_type(observed))
        self.assertEqual(GraphQLFloat, observed.of_type)

    def test_str(self):
        observed = self.encoder.build(str)

        self.assertTrue(is_non_null_type(observed))
        self.assertEqual(GraphQLString, observed.of_type)

    def test_bytes(self):
        observed = self.encoder.build(bytes)

        self.assertTrue(is_non_null_type(observed))
        self.assertEqual(GraphQLString, observed.of_type)

    def test_datetime(self):
        observed = self.encoder.build(datetime)

        self.assertTrue(is_non_null_type(observed))
        self.assertEqual(GraphQLString, observed.of_type)

    def test_timedelta(self):
        observed = self.encoder.build(timedelta)

        self.assertTrue(is_non_null_type(observed))
        self.assertEqual(GraphQLString, observed.of_type)

    def test_date(self):
        observed = self.encoder.build(date)

        self.assertTrue(is_non_null_type(observed))
        self.assertEqual(GraphQLString, observed.of_type)

    def test_time(self):
        observed = self.encoder.build(time)

        self.assertTrue(is_non_null_type(observed))
        self.assertEqual(GraphQLString, observed.of_type)

    def test_uuid(self):
        observed = self.encoder.build(UUID)

        self.assertTrue(is_non_null_type(observed))
        self.assertEqual(GraphQLID, observed.of_type)

    def test_model(self):
        # noinspection PyUnusedLocal
        class Foo(DeclarativeModel):
            """For testing purposes."""

            bar: str

        observed = self.encoder.build(Foo)

        self.assertTrue(is_non_null_type(observed))
        self.assertTrue(is_object_type(observed.of_type))
        self.assertEqual("Foo", observed.of_type.name)
        self.assertEqual(1, len(observed.of_type.fields))

        observed_field = observed.of_type.fields["bar"].type
        self.assertTrue(is_non_null_type(observed_field))
        self.assertEqual(GraphQLString, observed_field.of_type)

    def test_model_type(self):
        # noinspection PyUnusedLocal
        Foo = ModelType.build("Foo", {"bar": str})
        observed = self.encoder.build(Foo)

        self.assertTrue(is_non_null_type(observed))
        self.assertTrue(is_object_type(observed.of_type))
        self.assertEqual("Foo", observed.of_type.name)
        self.assertEqual(1, len(observed.of_type.fields))

        observed_field = observed.of_type.fields["bar"].type
        self.assertTrue(is_non_null_type(observed_field))
        self.assertEqual(GraphQLString, observed_field.of_type)

    def test_enum(self):
        class Foo(Enum):
            ONE = 1
            TWO = 2

        observed = self.encoder.build(Foo)
        self.assertTrue(is_non_null_type(observed))
        self.assertTrue(is_enum_type(observed.of_type))
        self.assertEqual("Foo", observed.of_type.name)

    def test_list(self):
        observed = self.encoder.build(list[int])

        self.assertTrue(is_non_null_type(observed))
        self.assertTrue(is_list_type(observed.of_type))

        observed_inner = observed.of_type.of_type
        self.assertTrue(is_non_null_type(observed_inner))
        self.assertEqual(GraphQLInt, observed_inner.of_type)

    def test_set(self):
        observed = self.encoder.build(set[int])

        self.assertTrue(is_non_null_type(observed))
        self.assertTrue(is_list_type(observed.of_type))

        observed_inner = observed.of_type.of_type
        self.assertTrue(is_non_null_type(observed_inner))
        self.assertEqual(GraphQLInt, observed_inner.of_type)

    def test_dict(self):
        observed = self.encoder.build(dict[str, int])

        self.assertTrue(is_non_null_type(observed))
        self.assertTrue(is_list_type(observed.of_type))

        observed_inner = observed.of_type.of_type
        self.assertTrue(is_object_type(observed_inner))
        self.assertEqual("DictItem", observed_inner.name)

        self.assertTrue(is_non_null_type(observed_inner.fields["key"].type))
        self.assertEqual(GraphQLString, observed_inner.fields["key"].type.of_type)

        self.assertTrue(is_non_null_type(observed_inner.fields["value"].type))
        self.assertEqual(GraphQLInt, observed_inner.fields["value"].type.of_type)


if __name__ == "__main__":
    unittest.main()
