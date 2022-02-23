import requests as requests


def test_add_coinbase_account():
    add_coin_response = requests.post(
        "http://localhost:5566/coinbase",
        json={
            "user": "a6ef81f1-8145-46e3-bd54-52713958cae3",
            "api_key": "6ribNaFzwywbwWaD",
            "api_secret": "r36KWJEtzA3eN4BDkdOYenCOK7QVsfpI"
        },
    )
