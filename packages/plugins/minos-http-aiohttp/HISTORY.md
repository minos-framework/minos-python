# History

## 0.6.0 (2022-03-28)

* Add `AioHttpConnector` as the implementation of the `aiohttp` server.
* Add `AioHttpRequest`, `AioHttpResponse` and `AioHttpResponseException` classes as the request/response wrappers for `aiohttp`.

## 0.7.0 (2022-05-11)

* Now `AioHttpRequest`'s `headers` attribute is mutable.
* Unify documentation building pipeline across all `minos-python` packages.
* Fix documentation building warnings.
* Fix bug related with package building and additional files like `AUTHORS.md`, `HISTORY.md`, etc.