import unittest

from tests.utils import (
    BASE_PATH,
)


class TestAPISpecs(unittest.TestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_true(self):
        self.assertEqual(True, True)

    def test_openapi(self):
        pass

    def test_asyncapi(self):
        pass


if __name__ == "__main__":
    unittest.main()
