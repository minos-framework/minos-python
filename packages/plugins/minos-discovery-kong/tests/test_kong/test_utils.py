import unittest

from minos.plugins.kong.utils import (
    Endpoint,
    PathPart,
)


class TestKongDiscoveryUtils(unittest.IsolatedAsyncioTestCase):
    def test_endpoint_constructor(self) -> None:
        endopoint = Endpoint("minos/plugin")
        self.assertIsInstance(endopoint, Endpoint)
        self.assertEqual(endopoint.path_as_str, "minos/plugin")
        self.assertEqual(endopoint.path[0].name, "minos")

    def test_path_constructor(self) -> None:
        pathpart = PathPart("minos")
        self.assertIsInstance(pathpart, PathPart)
        self.assertEqual(pathpart.name, "minos")
        self.assertEqual(pathpart.is_generic, False)

    def test_path_as_str(self) -> None:
        pathpart = Endpoint(r"/user/{uuid:\w{8}-\w{4}-\w{4}-\w{4}-\w{12}}")
        self.assertEqual(pathpart.path_as_str, "/user/.*")

    def test_path_as_regex(self) -> None:
        # pylint: disable=W605
        pathpart = Endpoint(r"/user/{uuid:\w{8}-\w{4}-\w{4}-\w{4}-\w{12}}")
        self.assertEqual(pathpart.path_as_regex, "/user/\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12}")

    def test_path_as_regex_simple_parameter(self) -> None:
        pathpart = Endpoint("/user/{uuid}")
        self.assertEqual(pathpart.path_as_regex, "/user/.*")

    def test_path_as_regex_parameter(self) -> None:
        pathpart = Endpoint("/user/{:uuid}")
        self.assertEqual(pathpart.path_as_regex, "/user/.*")

    def test_path_as_regex_multiple_parameter(self) -> None:
        pathpart = Endpoint("/user/{:uuid}/{:uuid}")
        self.assertEqual(pathpart.path_as_regex, "/user/.*/.*")

    def test_path_as_regex_uuid(self) -> None:
        # pylint: disable=W605
        pathpart = Endpoint(r"/user/{uuid:\w{8}-\w{4}-\w{4}-\w{4}-\w{12}}/test")
        self.assertEqual(pathpart.path_as_regex, "/user/\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12}/test")
