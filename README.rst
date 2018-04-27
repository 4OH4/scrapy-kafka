scrapy-kafka
============

Kafka-based components for Scrapy. There are 2 components:

- A custom ``Spider`` that waits for HTML Responses to crawl via a Kafka topic.
When there are no more messages to read for the topic, the ``Spider`` just stays idle.
- A custom ``ItemPipeline`` component that stores a JSON-ified ``Item`` back into another Kafka topic.

Please see the `example`_ directory for how to use this.

.. _example: https://github.com/dfdeshom/scrapy-kafka/tree/master/example

Contributors
-------------
Contributors to `scrapy-kafka`, listed alphabetically:

* Matthew Daniel `@mdaniel`_
* Didier Deshommes `@dfdeshom`_
* Akshay Philar `@akshayphilar`_

.. _@mdaniel: https://github.com/mdaniel
.. _@dfdeshom: https://github.com/dfdeshom
.. _@akshayphilar: https://github.com/akshayphilar