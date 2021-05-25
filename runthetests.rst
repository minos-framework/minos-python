Run the tests
---------------

In order to run the tests, please make sure you have the `Docker Engine <https://docs.docker.com/engine/install/>`_ and `Docker Compose <https://docs.docker.com/compose/install/>`_ installed.

Move into tests/ directory

.. code-block:: bash

    cd tests/

Run service dependencies:

.. code-block:: bash

    docker-compose up -d

Install library dependencies:

.. code-block:: bash

    make install

Run tests:

.. code-block:: bash

    make test
