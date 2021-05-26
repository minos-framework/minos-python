History
==========

0.0.1.1-alpha (2021-03-28)
--------------------------------

* First release on PyPI.


0.0.1.2-alpha (2021-03-31)
-----------------------------

* Completed the Generic Config class Microservices.
* README badges modification.


0.0.1.3-alpha (2021-03-31)
----------------------------

* Updated Markdown for README and HISTORY

0.0.1.4-alpha (2021-04-02)
------------------------------

* Completed the Config Class for YAML Support
* Added a set of tests for the MinosConfig class
* As well from the tests folder would be possible to get and example of
  the configuration file for microservices

0.0.1.5-alpha (2021-04-02)
----------------------------

* Added support for database configuration parameters ( events and commands)
* Added set of tests for database config parameters

0.0.1.6 (2021-04-03)
---------------------

* MinosConfig added service parameter

0.0.1.7 (2021-04-06)
----------------------

* Modified the schema structure of Avro generic encoder and decoder


0.0.2 (2021-04-19)
-------------------

* Added support for Model classe
* Added Custom Fields for Minos Model Class
* Added validation attributes for model class ( validation from typing )
* Modified Avro Encoder/Decoder
* Lot of improvements and corrections


0.0.3 (2021-04-26)
--------------------

* Added fastavro support
* Completed Aggregate Model
* Added EventModel and CommandModel classes
* Improved MinosConfig
* added some utilities for internal use
* added Storage Event support for Aggregate

0.0.4 (2021-04-28)
--------------------

* Bug Corrections
* EventModel and CommandModel reformat
* Added support for ModelRef in MinosModel
* Improved MinosConfig

0.0.5 (2021-05-03)
--------------------

* Added PostgreSQL base class for pool connections support
* Added created_at column for Aggregate Event table
* Modified MinosConfig for Snapshot support
* Added database tables setup method

0.0.6 (2021-05-04)
--------------------


0.0.7 (2021-05-06)
--------------------

* Moved CommandModel and EventModel to the model module
* Created CommandReplyModel for Saga reply

0.0.8 (2021-05-07)
--------------------

* Added SAGA Attriubutes in MinosConfig

0.0.9 (2021-05-10)
-------------------

* Added Config Parameters for MinosConfig

0.0.10 (2021-05-11)
---------------------

* Added JSON Binary format
* Improved LMDB support for Saga Binary format

0.0.11 (2021-05-12)
---------------------

* EventModel modified the list of items from Aggregate to MinosModel, for better compatibility
* Added DB Storage Abstract class
* Added code documentation

0.0.12 (2021-05-17)
---------------------

* Added Support for Poetry
* Added action for Doc generation

0.0.13 (2021-05-18)
---------------------

* Added Abstract classes for Saga


0.0.14 (2021-05-20)
--------------------

* Added Dependency Injections Containers
* Modified .gitignore
* Moved Borker Class from static method impl to instance by init

0.0.15 (2021-05-26)
--------------------

* Some code refactoring
* Test cases coverage optimization
* fixed some Sagas functionalities
* fixed Sphinx documentation generation process
