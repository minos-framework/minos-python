import time

import requests as requests


def test_add_cart():
    add_c_response = requests.post(
        "http://localhost:5566/cart",
        json={
            "customer": "a6ef81f1-8145-46e3-bd54-52713958cae3",
        },
    )

    assert add_c_response.status_code == 200
    content = add_c_response.json()
    uuid = content["uuid"]
    time.sleep(1)
    get_c_response = requests.get("http://localhost:5566/cart/{}".format(uuid))

    content_get = get_c_response.json()
    assert content_get["uuid"] == uuid
    assert get_c_response.status_code == 200
