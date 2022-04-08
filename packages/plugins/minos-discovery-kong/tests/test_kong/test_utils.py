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
