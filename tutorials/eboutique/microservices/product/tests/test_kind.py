import unittest
import time

import requests

from .minos_testcase import MinosTestCase


class TestKind(MinosTestCase):
    def test_something(self):
        self.assertEqual(True, True)  # add assertion here

    def test_add_product(self):
        add_p_response = requests.post(
            "http://localhost:5566/product",
            json={
                "title": "Product One",
                "description": "Product One in our catalog",
                "picture": "images / product.jpeg",
                "categories": [{"title": "tshirt"}, {"title": "casual"}],
                "price": {"currency": "EUR", "units": 59},
            },
        )

        assert add_p_response.status_code == 200
        content = add_p_response.json()
        uuid = content["uuid"]
        time.sleep(1)
        get_p_response = requests.get("http://localhost:5566/product/{}".format(uuid))

        content_get = get_p_response.json()
        assert content_get["uuid"] == uuid
        assert content_get["title"] == "Product One"


if __name__ == '__main__':
    unittest.main()
