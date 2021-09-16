import unittest
from typing import (
    Optional,
)

from minos.common import (
    Field,
    MinosAttributeValidationException,
    MinosMalformedAttributeException,
    MinosParseAttributeException,
    MinosReqAttributeException,
    MinosTypeAttributeException,
    ModelType,
)
from tests.model_classes import (
    Analytics,
    Customer,
    CustomerFailDict,
    CustomerFailList,
    GenericUser,
    ShoppingList,
    T,
    User,
)


class TestDeclarativeModel(unittest.TestCase):
    def test_constructor_args(self):
        model = Customer(1234, "johndoe", "John", "Doe")
        self.assertEqual(1234, model.id)
        self.assertEqual("johndoe", model.username)
        self.assertEqual("John", model.name)
        self.assertEqual("Doe", model.surname)

    def test_constructor_multiple_values(self):
        with self.assertRaises(TypeError):
            Customer(1234, id=1234)
        with self.assertRaises(TypeError):
            Customer(None, id=1234)

    def test_constructor_kwargs(self):
        model = Customer(id=1234, username="johndoe", name="John", surname="Doe")
        self.assertEqual(1234, model.id)
        self.assertEqual("johndoe", model.username)
        self.assertEqual("Doe", model.surname)
        self.assertEqual("John", model.name)

    def test_getattr(self):
        model = Customer(1234, name="John", surname="Doe")
        self.assertEqual(1234, model.id)
        self.assertEqual("Doe", model.surname)
        self.assertEqual("John", model.name)

    def test_setattr(self):
        expected = Customer(1234, name="John", surname="Doe")

        observed = Customer(1234)
        observed.name = "John"
        observed.surname = "Doe"

        self.assertEqual(expected, observed)

    def test_setattr_raises(self):
        model = Customer(123)
        with self.assertRaises(AttributeError):
            model.address = "str kennedy"

    def test_getattr_raises(self):
        model = Customer(123)
        with self.assertRaises(AttributeError):
            model.address

    def test_getitem(self):
        model = Customer(1234, name="John", surname="Doe")
        self.assertEqual(1234, model["id"])
        self.assertEqual("Doe", model["surname"])
        self.assertEqual("John", model["name"])

    def test_getitem_raises(self):
        model = Customer(1234)
        with self.assertRaises(KeyError):
            model["failure"]

    def test_setitem(self):
        expected = Customer(1234, name="John", surname="Doe")

        observed = Customer(1234)
        observed["name"] = "John"
        observed["surname"] = "Doe"

        self.assertEqual(expected, observed)

    def test_setitem_raises(self):
        model = Customer(1234)
        with self.assertRaises(KeyError):
            model["failure"] = ""

    def test_attribute_parser_same_type(self):
        model = Customer(1234, name="john")
        self.assertEqual("John", model.name)

    def test_attribute_parser_different_type(self):
        model = ShoppingList(User(1234), cost="1.234,56")
        self.assertEqual(1234.56, model.cost)

    def test_attribute_parser_attribute(self):
        model = ShoppingList(User(1234, "admin"), cost="1.234,56")
        self.assertEqual(0.0, model.cost)

    def test_aggregate_int_as_string_type_setter(self):
        model = Customer("1234")
        model.name = "John"
        self.assertEqual(1234, model.id)
        self.assertEqual("John", model.name)

    def test_aggregate_wrong_int_type_setter(self):
        with self.assertRaises(MinosTypeAttributeException):
            Customer("1234S")

    def test_aggregate_string_type_setter(self):
        model = Customer(123)
        model.name = "John"
        self.assertEqual("John", model.name)

    def test_aggregate_wrong_string_type_setter_with_parser(self):
        model = Customer(123)
        with self.assertRaises(MinosParseAttributeException):
            model.name = 456

    def test_aggregate_wrong_string_type_setter(self):
        model = Customer(123)
        with self.assertRaises(MinosTypeAttributeException):
            model.surname = 456

    def test_aggregate_bool_type_setter(self):
        model = Customer(123)
        model.name = "John"
        model.is_admin = True
        self.assertTrue(model.is_admin)

    def test_aggregate_wrong_bool_type_setter(self):
        model = Customer(123)
        model.name = "John"
        with self.assertRaises(MinosTypeAttributeException):
            model.is_admin = "True"

    def test_aggregate_empty_mandatory_field(self):
        with self.assertRaises(MinosReqAttributeException):
            Customer()

    def test_model_list_class_attribute(self):
        model = Customer(123)
        model.lists = [1, 5, 8, 6]

        self.assertEqual([1, 5, 8, 6], model.lists)

    def test_model_list_wrong_attribute_type(self):
        model = Customer(123)
        with self.assertRaises(MinosTypeAttributeException):
            model.lists = [1, "hola", 8, 6]

    def test_model_ref(self):
        shopping_list = ShoppingList(cost=3.14)
        user = User(1234)
        shopping_list.user = user
        self.assertEqual(user, shopping_list.user)

    def test_model_ref_raises(self):
        shopping_list = ShoppingList(cost=3.14)
        with self.assertRaises(MinosTypeAttributeException):
            shopping_list.user = "foo"

    def test_recursive_type_composition(self):
        orders = {
            "foo": [ShoppingList(User(1)), ShoppingList(User(1))],
            "bar": [ShoppingList(User(2)), ShoppingList(User(2))],
        }

        analytics = Analytics(1, orders)
        self.assertEqual(orders, analytics.orders)

    def test_model_fail_list_class_attribute(self):
        with self.assertRaises(MinosMalformedAttributeException):
            CustomerFailList(["foo", "bar"])

    def test_model_fail_dict_class_attribute(self):
        with self.assertRaises(MinosMalformedAttributeException):
            CustomerFailDict({"foo": "bar"})

    def test_empty_required_value(self):
        with self.assertRaises(MinosReqAttributeException):
            User()

    def test_validate_required_raises(self):
        with self.assertRaises(MinosAttributeValidationException):
            User(-34)

    def test_validate_optional_raises(self):
        with self.assertRaises(MinosAttributeValidationException):
            User(1234, username="user name")

    def test_fields(self):
        user = User(123)
        fields = {
            "id": Field("id", int, 123, validator=user.validate_id),
            "username": Field("username", Optional[str], parser=user.parse_username, validator=user.validate_username),
        }
        self.assertEqual(fields, user.fields)

    def test_equal(self):
        a, b = User(123), User(123)
        self.assertEqual(a, b)

    def test_complex_equal(self):
        a, b = ShoppingList(User(1234), cost="1.234,56"), ShoppingList(User(1234), cost="1.234,56")
        self.assertEqual(a, b)

    def test_not_equal(self):
        a, b = User(123), User(456)
        self.assertNotEqual(a, b)

    def test_iter_list(self):
        user = User(123)
        expected = ["id", "username"]

        self.assertEqual(expected, list(user))

    def test_iter_dict(self):
        user = User(123)
        expected = {"id": 123, "username": None}
        self.assertEqual(expected, dict(user))

    def test_len(self):
        user = User(123)
        self.assertEqual(2, len(user))

    def test_hash(self):
        user = User(123)

        expected = hash(
            (
                Field("id", int, 123, validator=user.validate_id),
                Field("username", Optional[str], parser=user.parse_username, validator=user.validate_username),
            )
        )
        self.assertEqual(expected, hash(user))

    def test_repr(self):
        shopping_list = ShoppingList(User(1234), cost="1.234,56")
        expected = "ShoppingList(user=User(id=1234, username=None), cost=1234.56)"
        self.assertEqual(expected, repr(shopping_list))

    def test_str(self):
        shopping_list = ShoppingList(User(1234), cost="1.234,56")
        self.assertEqual(repr(shopping_list), str(shopping_list))

    def test_classname_cls(self):
        self.assertEqual("tests.model_classes.Customer", Customer.classname)

    def test_classname_instance(self):
        model = Customer(1234, "johndoe", "John", "Doe")
        self.assertEqual("tests.model_classes.Customer", model.classname)

    def test_model_type(self):
        model = User(1234, "johndoe")
        self.assertEqual(
            ModelType.build("tests.model_classes.User", {"id": int, "username": Optional[str]}), model.model_type
        )

    def test_type_type_hints_parameters(self):
        self.assertEqual((T,), GenericUser.type_hints_parameters)

    def test_type_hints_parameters_empty(self):
        self.assertEqual(tuple(), User.type_hints_parameters)


if __name__ == "__main__":
    unittest.main()
