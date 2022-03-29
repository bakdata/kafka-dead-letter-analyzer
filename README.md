[![Build Status](https://dev.azure.com/bakdata/public/_apis/build/status/bakdata.kafka-dead-letter-analyzer?repoName=bakdata%2Fkafka-dead-letter-analyzer&branchName=main)](https://dev.azure.com/bakdata/public/_build/latest?definitionId=31&repoName=bakdata%2Fkafka-dead-letter-analyzer&branchName=main)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Adead-letter-analyzer&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=com.bakdata.kafka%3Adead-letter-analyzer)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Adead-letter-analyzer&metric=coverage)](https://sonarcloud.io/summary/new_code?id=com.bakdata.kafka%3Adead-letter-analyzer)
[![Maven](https://img.shields.io/maven-central/v/com.bakdata.kafka/dead-letter-analyzer.svg)](https://search.maven.org/search?q=g:com.bakdata.kafka%20AND%20a:dead-letter-analyzer&core=gav)

# kafka-dead-letter-analyzer

A Kafka Streams application that analyzes dead letters in your Kafka cluster.

Dead letter analyzer supports three different types of dead letters in your Kafka cluster:

- [Kafka Connect dead letters](https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues/)
- Kafka Streams dead letters in a similar format to Kafka Connect generated
  using [kafka-error-handling](https://github.com/bakdata/kafka-error-handling)
- Kafka Streams dead letters in Apache Avro format generated using [kafka-error-handling](https://github.com/bakdata/kafka-error-handling)

It picks up all these dead letters and aggregates them by identifying different error types.
Aggregations are done by topic and are used to collect statistics for an error.
This includes the first, last, and number of occurrences of an error.
These statistics can then be stored in an external data system, such as a relational database or Elasticsearch,
using Kafka Connect or be directly explored in the respective Kafka topic.
Furthermore, for each error type, one example record is selected in order to better understand what the error is about.
Finally, all dead letters are sent to a topic in a standardized format along with its Kafka message context
(topic, partition, offset, timestamp, key).
This data can also be stored in an external data system, such as Elasticsearch,
to further explore the errors occurring in your data pipelines.

## Getting started

The dead letter analyzer is available on [Docker Hub](https://hub.docker.com/repository/docker/bakdata/kafka-dead-letter-analyzer).
You can simply run it using docker:

```
docker run bakdata/kafka-dead-letter-analyzer \
    --brokers localhost:9092 \
    --schema-registry-url http://localhost:8081 \
    --input-pattern .*-dead-letters \
    --output-topic dead-letters-all \
    --extra-output-topics stats=dead-letter-stats \
    --extra-output-topics examples=dead-letter-examples \
    --error-topic dead-letter-analyzer-dead-letters
```

The analyzer then reads dead letters from any topic that matches the pattern `.*-dead-letters`.
These can be in any of the above-mentioned formats and the application automatically detects the serialization format.
The aggregated statistics are produced to the `dead-letter-stats` topic.
Examples are forwarded to `dead-letter-examples` and all dead letters are sent to `dead-letters-all`.
If any processing error occurs in the analyzer itself, a dead letter is sent to `dead-letter-analyzer-dead-letters`.
These dead letters will be analyzed by the application as well.

Messages sent to the examples and all topic are serialized using [kafka-large-message-serde](https://github.com/bakdata/kafka-large-message-serde).

## Usage

This application is built using [streams-bootstrap](https://github.com/bakdata/streams-bootstrap) and can be configured as documented.

We suggest running your application on Kubernetes using Helm.
You can use the chart provided with [streams-bootstrap](https://github.com/bakdata/streams-bootstrap/tree/master/charts/streams-app).

## Kafka Connect

If you want to store the analysis results in an external data system, such as Elasticsearch, you can do so using Kafka Connect.
We provide examples for each of the three output topics.
However, they can be easily adapted to any data system supported by Kafka Connect.

`dead-letters-all`:
```json
{
    "behavior.on.malformed.documents": "warn",
    "connection.password": "password",
    "connection.url": "http://localhost:9243",
    "connection.username": "username",
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.deadletterqueue.topic.name": "dead-letters-all-elasticsearch-sink-dead-letters",
    "errors.tolerance": "all",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.ignore": "false",
    "name": "dead-letters-all-elasticsearch-sink",
    "tasks.max": "1",
    "topics": "dead-letters-all",
    "transforms": "changeTopic",
    "transforms.changeTopic.timestamp.format": "yyyyMMdd",
    "transforms.changeTopic.topic.format": "dead-letters-all-${timestamp}",
    "transforms.changeTopic.type": "org.apache.kafka.connect.transforms.TimestampRouter",
    "type.name": "_doc",
    "value.converter": "com.bakdata.kafka.LargeMessageConverter",
    "value.converter.large.message.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://localhost:8081"
}
```
This connector partitions the dead letters by day so that you can set up Index Lifecycle Management for dropping old errors.

`dead-letter-stats`:
```json
{
    "behavior.on.malformed.documents": "warn",
    "connection.password": "password",
    "connection.url": "http://localhost:9243",
    "connection.username": "username",
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.deadletterqueue.topic.name": "dead-letter-stats-elasticsearch-sink-dead-letters",
    "errors.tolerance": "all",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.ignore": "false",
    "name": "dead-letter-stats-elasticsearch-sink",
    "tasks.max": "1",
    "topics": "dead-letter-stats",
    "transforms": "changeTopic",
    "transforms.changeTopic.regex": ".*",
    "transforms.changeTopic.replacement": "dead-letter-stats",
    "transforms.changeTopic.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "type.name": "_doc",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://localhost:8081",
    "write.method": "upsert"
}
```

`dead-letter-examples`:
```json
{
    "behavior.on.malformed.documents": "warn",
    "connection.password": "password",
    "connection.url": "http://localhost:9243",
    "connection.username": "username",
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.deadletterqueue.topic.name": "dead-letter-examples-elasticsearch-sink-dead-letters",
    "errors.tolerance": "all",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.ignore": "false",
    "name": "dead-letter-examples-elasticsearch-sink",
    "tasks.max": "1",
    "topics": "dead-letter-examples",
    "transforms": "changeTopic",
    "transforms.changeTopic.regex": ".*",
    "transforms.changeTopic.replacement": "dead-letter-stats",
    "transforms.changeTopic.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "type.name": "_doc",
    "value.converter": "com.bakdata.kafka.LargeMessageConverter",
    "value.converter.large.message.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://localhost:8081",
    "write.method": "upsert"
}
```
These two connectors use upsert semantics to store statistics alongside examples in the `dead-letter-stats` Elasticsearch index.

## Development

If you want to contribute to this project, you can simply clone the repository and build it via Gradle.
All dependencies should be included in the Gradle files, there are no external prerequisites.

```bash
> git clone git@github.com:bakdata/kafka-dead-letter-analyzer.git
> cd kafka-dead-letter-analyzer && ./gradlew build
```

Please note, that we have [code styles](https://github.com/bakdata/bakdata-code-styles) for Java.
They are basically the Google style guide, with some small modifications.

## Contributing

We are happy if you want to contribute to this project.
If you find any bugs or have suggestions for improvements, please open an issue.
We are also happy to accept your PRs.
Just open an issue beforehand and let us know what you want to do and why.

## License
This project is licensed under the MIT license.
Have a look at the [LICENSE](https://github.com/bakdata/kafka-dead-letter-analyzer/blob/main/LICENSE) for more details.
