import logging

import time

import pytest
import requests as requests

logger = logging.getLogger(__name__)

@pytest.fixture
def add_product():
    add_p_response = requests.post(
        "http://localhost:5566/product",
        json={
            "title": "Product For Cart",
            "description": "Product used during Cart Test",
            "picture": "images/product.jpeg",
            "categories": [{"title": "tshirt"}, {"title": "casual"}],
            "price": {"currency": "EUR", "units": 59},
        },
    )
    content = add_p_response.json()
    return content['uuid']

@pytest.fixture
def add_cart():
    add_c_response = requests.post(
        "http://localhost:5566/cart",
        json={
            "user": "a6ef81f1-8145-46e3-bd54-52713958cae3",
        },
    )
    content = add_c_response.json()
    return content['uuid']


def test_add_cart():
    add_c_response = requests.post(
        "http://localhost:5566/cart",
        json={
            "user": "a6ef81f1-8145-46e3-bd54-52713958cae3",
        },
    )
    assert add_c_response.status_code == 200
    content = add_c_response.json()
    uuid = content["uuid"]
    time.sleep(1)
    get_c_response = requests.get("http://localhost:5566/cart/{}".format(uuid))

    assert get_c_response.status_code == 200
    content_get = get_c_response.json()
    assert content_get["uuid"] == uuid


def test_add_item_to_cart(add_product, add_cart):
    product_uid = add_product
    cart_id = add_cart
    add_c_i_response = requests.post(
        "http://localhost:5566/cart/{}/item".format(cart_id), json={"product": product_uid, "quantity": 2}
    )
    assert add_c_i_response.status_code == 200
    time.sleep(6)
    get_c_response = requests.get("http://localhost:5566/cart/{}/items".format(cart_id))
    content_get = get_c_response.json()
    for item in content_get:
        assert item["product"]['uuid'] == product_uid
