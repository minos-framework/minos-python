# Architecture overview

Segun el Microservice Patterns Language, la arquitectura de Microservicios se compone en unos set de patrones y estos se agrupan
en en tres diferentes segmentos:

* Splitting Patterns Set
* Reassembling Patterns Set
* Operations Patterns Set

## Splitting Patterns

estos patrones realmente no afectan directamente el desarollar, son patrones que se utilizan para definir el
diseño de la aplicacion y sus servicios. En este documento no trataremos ninguno de estos patrones porque estan fuera
de su alcance.

## Reassemblig Patterns

Estos patrones afectan directamente el desarollador, porque son los patrones que manejan la comunicacion de los microservicios
entre ellos y la manera en que es almacenado el dato y como se pueden efectuar llamadas al dato.

Los patrones de este segmento se dividen en:

* Data Patterns:
  * Database Architecture: One Database per Service
  * Querying: CQRS
  * Data Consistency
    * Aggregate
    * Saga
    * Event Sourcing
    * Domain Event
* Communication Patterns:
  * Transactional Outbox
    * Polling publisher
  * Messaging
    * Remote Procedure Invocation
      * Circuit Breaker
* Discovery
  * Server Side discovery
* External API
  * API Gateway


## Operations Patterns

Minos ofrece un cli con una serie de patrones desarollados, pero estos patrones
no afectan directamente el desarollador sino en la manera de effectuar el deploy
del microservicio mismo. En este documento no trataremos ninguno de estos patrones porque estan fuera
de su alcance.

