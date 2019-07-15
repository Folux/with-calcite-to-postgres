# Example Apache Calcite
Example using Apache Calcite to connect to a local Postgres DB and to create some relational algebra on the `foodmart` sample schema and to execute it. Also a CSV sample schema is provided and with a  relational algebra queried on it.

## Getting Started

Simplest way to run the code is to get the prepared jar from the `/src/run/` folder.
The program expects to arguments.
1. A JSON file pointing to a provided CSV sample schema
2. A JSON file pointing to your Postgres server 

For the JSON files use the files in the `/src/main/resources/` folder. Modify the template before to connect to your Postgres server-

### Prerequisites

* A Postgres server set up and running.
* The `foodmart` sample schema loaded on the Postgres server. The database for the schema must be called `foodmart`.
* A JSON file to the schema for your JDBC connection must be configured for your `foodmart` DB. Use the template `/src/main/resources/foodMartSchemaTemplate.json` to get it in the correct format.
* If you don't want to run it from an IDE or you don't want to build the code yourself, use the prepared jar `/src/run/calcite-postgres-1.0-jar-with-dependencies.jar`

### How to Run

```java -jar path/to/your/jar/calcite-postgres-1.0-jar-with-dependencies.jar ../path/to/your/json/foodMartSchema.json```
