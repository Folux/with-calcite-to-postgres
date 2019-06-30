# Example Apache Calcite
Example using Apache Calcite to connect to a local Postgres DB and to create some relational algebra on the `foodmart` sample schema and to execute it.

## Getting Started

Simplest way to run the code is to get the prepared jar from the `/src/run/` folder, and use the modified version of the schema JSON in the `/src/main/resources/` folder.

### Prerequisites

* A Postgres server set up and running.
* The `foodmart` sample schema loaded on the Postgres server. The database for the schema must be called `foodmart`.
* A JSON file to the schema for your JDBC connection must be configured for your `foodmart` DB. Use the template `/src/main/resources/foodMartSchemaTemplate.json` to get it in the correct format.
* If you don't want to run it from an IDE or you don't want to build the code yourself, use the prepared jar `/src/run/calcite-postgres-1.0-jar-with-dependencies.jar`

### How to Run

```java -jar path/to/your/jar/calcite-postgres-1.0-jar-with-dependencies.jar ../path/to/your/json/foodMartSchema.json```
