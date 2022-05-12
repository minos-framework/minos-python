# History

## 0.7.0 (2022-05-11)

* Add `KongClient` as a class to interact with the `kong` API Gateway.
* Add `KongDiscoveryClient` as the `minos.networks.DiscoveryClient` implementation for the `kong` API Gateway.
* Add `middleware` function to automatically extract the user identifier from request's header variable set by the `kong` API Gateway. 