# weather-mon-docs-api
Weather Monitoring using SG Docs API

Stargate is a data gateway deployed between the client applications and a database. The Stargate Document API modifies and queries data stored as unstructured JSON documents in collections. This gives the application developers native json support without having to give up any of the reliability and scalability goodness of Cassandra. This Stargate Docs API lets most Cassandra distros (Cassandra 3.11, Cassandra 4.0, and DataStax Enterprise 6.8), work with JSON through a REST API. The deep dive details on how to the Stargate Docs API stores the json as C* column family within Cassandra are at  https://stargate.io/2020/10/19/the-stargate-cassandra-documents-api.html.

In this blog, you would see how to use the Stargate Docs API and build a simple TimeSeries DB for Weather monitoring on top of the DataStax Astra DB. The demo is restricted to the data model, data extraction, storage and retrieval queries for the Weather monitoring app.

The retrieval queries for various reports are available as Postman collection at https://www.postman.com/collections/7e88080ccd5b69f7d633


