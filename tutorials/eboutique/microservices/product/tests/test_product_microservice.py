import time

import requests as requests


def test_add_product():
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


def test_get_all_products():
    add_p_response = requests.get("http://localhost:5566/products")

    assert add_p_response.status_code == 200
    content = add_p_response.json()
    assert len(content) > 0
