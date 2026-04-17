/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.kafka;

import static com.bakdata.kafka.ConnectDeadLetterParserTest.toBytes;
import static com.bakdata.kafka.DeadLetterAnalyzerTopology.EXAMPLES_TOPIC_LABEL;
import static com.bakdata.kafka.DeadLetterAnalyzerTopology.STATS_TOPIC_LABEL;
import static com.bakdata.kafka.DescribingProcessingExceptionHandler.HEADER_ERRORS_PROCESSOR_NODE_ID_NAME;
import static com.bakdata.kafka.DescribingProcessingExceptionHandler.HEADER_ERRORS_TASK_ID_NAME;
import static com.bakdata.kafka.ErrorHeaderProcessor.DESCRIPTION;
import static com.bakdata.kafka.ErrorHeaderProcessor.EXCEPTION_CLASS_NAME;
import static com.bakdata.kafka.ErrorHeaderProcessor.EXCEPTION_MESSAGE;
import static com.bakdata.kafka.ErrorHeaderProcessor.EXCEPTION_STACK_TRACE;
import static com.bakdata.kafka.ErrorHeaderProcessor.OFFSET;
import static com.bakdata.kafka.ErrorHeaderProcessor.PARTITION;
import static com.bakdata.kafka.ErrorHeaderProcessor.TOPIC;
import static com.bakdata.kafka.Formatter.DATE_TIME_FORMATTER;
import static com.bakdata.kafka.HeaderLargeMessagePayloadProtocol.getHeaderName;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_CONNECTOR_NAME;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_EXCEPTION;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_EXCEPTION_MESSAGE;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_EXCEPTION_STACK_TRACE;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_EXECUTING_CLASS;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_ORIG_OFFSET;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_ORIG_PARTITION;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_ORIG_TOPIC;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_STAGE;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_TASK_ID;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_MESSAGE_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_OFFSET_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_PARTITION_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_STACKTRACE_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_TOPIC_NAME;

import com.bakdata.fluent_kafka_streams_tests.TestInput;
import com.bakdata.fluent_kafka_streams_tests.TestOutput;
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.bakdata.kafka.streams.ConfiguredStreamsApp;
import com.bakdata.kafka.streams.StreamsApp;
import com.bakdata.kafka.streams.StreamsAppConfiguration;
import com.bakdata.kafka.streams.StreamsTopicConfig;
import com.bakdata.kafka.streams.TestTopologyFactory;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(SoftAssertionsExtension.class)
class DeadLetterAnalyzerTopologyTest {
    private static final StreamsTopicConfig TOPIC_CONFIG = StreamsTopicConfig.builder()
            .inputPattern(Pattern.compile(".*-dead-letter-topic"))
            .outputTopic("output")
            .errorTopic("analyzer-stream-dead-letter-topic")
            .labeledOutputTopics(
                    Map.of(
                            EXAMPLES_TOPIC_LABEL, "examples",
                            STATS_TOPIC_LABEL, "stats"
                    )
            )
            .build();
    private final TestTopologyFactory factory = TestTopologyFactory.withSchemaRegistry();
    @RegisterExtension
    TestTopologyExtension<String, DeadLetter> topology = this.factory.createTopologyExtension(createApp());
    @InjectSoftAssertions
    private SoftAssertions softly;

    private static ConfiguredStreamsApp<StreamsApp> createApp() {
        return new ConfiguredStreamsApp<>(new DeadLetterAnalyzerApplication(),
                new StreamsAppConfiguration(TOPIC_CONFIG));
    }

    private static LocalDateTime parseDateTime(final String dateTime) {
        return LocalDateTime.parse(dateTime, DATE_TIME_FORMATTER);
    }

    private static LocalDateTime toDateTime(final Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
    }

    private static <T> ProducerRecord<String, T> deserializeNonBacked(final Serde<? extends T> valueSerde,
            final ProducerRecord<String, byte[]> record) {
        final Headers headers = new RecordHeaders(record.headers())
                .add(getHeaderName(false), new byte[]{FlagHelper.IS_NOT_BACKED});
        final String topic = record.topic();
        final byte[] value = record.value();
        final T deserializedValue = valueSerde.deserializer().deserialize(topic, headers, value);
        final Integer partition = record.partition();
        final Long timestamp = record.timestamp();
        final String key = record.key();
        return new ProducerRecord<>(topic, partition, timestamp, key, deserializedValue, headers);
    }

    @Test
    void shouldProcessDeadLetter() {
        final TestInput<String, SpecificRecord> input = this.getStreamsInput(Serdes.String());
        final TestOutput<String, FullDeadLetterWithContext> processedDeadLetters =
                this.getProcessedDeadLetters();
        final TestOutput<String, FullErrorStatistics> statistics = this.getStatistics();
        final TestOutput<String, ErrorExample> examples = this.getExamples();

        final DeadLetter deadLetter = DeadLetter.newBuilder()
                .setInputValue("foo")
                .setCause(ErrorDescription.newBuilder()
                        .setMessage("message")
                        .setStackTrace(StackTraceClassifierTest.STACK_TRACE)
                        .build())
                .setDescription("description")
                .setInputTimestamp(Instant.ofEpochMilli(200L))
                .build();
        final long timestamp = 0L;
        input.add("key", deadLetter, timestamp);
        this.softly.assertThat(processedDeadLetters.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).isEqualTo("my-stream-dead-letter-topic+0+0");
                    final FullDeadLetterWithContext value = producerRecord.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("key");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getOffset()).isEqualTo(0L);
                    this.softly.assertThat(value.getDeadLetter()).isEqualTo(deadLetter);
                    this.softly.assertThat(parseDateTime(value.getTimestamp()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(timestamp)));
                });
        this.softly.assertThat(statistics.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final FullErrorStatistics value = producerRecord.value();
                    this.softly.assertThat(value.getCount()).isEqualTo(1);
                    this.softly.assertThat(parseDateTime(value.getCreated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(timestamp)));
                    this.softly.assertThat(parseDateTime(value.getUpdated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(timestamp)));
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                });
        this.softly.assertThat(examples.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final ErrorExample value = producerRecord.value();
                    final ExampleDeadLetterWithContext example = value.getExample();
                    this.softly.assertThat(example.getDeadLetter()).isEqualTo(deadLetter);
                    this.softly.assertThat(example.getKey()).isEqualTo("key");
                    this.softly.assertThat(example.getOffset()).isEqualTo(0L);
                    this.softly.assertThat(example.getPartition()).isEqualTo(0);
                    this.softly.assertThat(parseDateTime(example.getTimestamp()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(timestamp)));
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                });
        this.assertNoDeadLetters();
    }

    @Test
    void shouldAggregateStatistics() {
        final TestInput<String, SpecificRecord> input = this.getStreamsInput(Serdes.String());
        final TestOutput<String, FullErrorStatistics> statistics = this.getStatistics();

        final DeadLetter firstDeadLetter = DeadLetter.newBuilder()
                .setInputValue("foo")
                .setCause(ErrorDescription.newBuilder()
                        .setMessage("message")
                        .setStackTrace(StackTraceClassifierTest.STACK_TRACE)
                        .build())
                .setDescription("description")
                .setInputTimestamp(Instant.ofEpochMilli(200L))
                .build();
        final long firstTimestamp = 0L;
        input.add("key", firstDeadLetter, firstTimestamp);
        this.softly.assertThat(statistics.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final FullErrorStatistics value = producerRecord.value();
                    this.softly.assertThat(value.getCount()).isEqualTo(1);
                    this.softly.assertThat(parseDateTime(value.getCreated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(parseDateTime(value.getUpdated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                });

        final DeadLetter secondDeadLetter = DeadLetter.newBuilder()
                .setInputValue("foo")
                .setCause(ErrorDescription.newBuilder()
                        .setMessage("message")
                        .setStackTrace(StackTraceClassifierTest.STACK_TRACE)
                        .build())
                .setDescription("description")
                .build();
        final long secondTimestamp = 10L;
        input.add("key", secondDeadLetter, secondTimestamp);
        this.softly.assertThat(statistics.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final FullErrorStatistics value = producerRecord.value();
                    this.softly.assertThat(value.getCount()).isEqualTo(2);
                    this.softly.assertThat(parseDateTime(value.getCreated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(parseDateTime(value.getUpdated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(secondTimestamp)));
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                });
        this.assertNoDeadLetters();
    }

    @Test
    void shouldOnlyForwardFirstExample() {
        final TestInput<String, SpecificRecord> input = this.getStreamsInput(Serdes.String());
        final TestOutput<String, FullDeadLetterWithContext> processedDeadLetters =
                this.getProcessedDeadLetters();
        final TestOutput<String, ErrorExample> examples = this.getExamples();

        final DeadLetter firstDeadLetter = DeadLetter.newBuilder()
                .setInputValue("foo")
                .setCause(ErrorDescription.newBuilder()
                        .setMessage("message")
                        .setStackTrace(StackTraceClassifierTest.STACK_TRACE)
                        .build())
                .setDescription("description")
                .setInputTimestamp(Instant.ofEpochMilli(200L))
                .build();
        final long firstTimestamp = 0L;
        input.add("key", firstDeadLetter, firstTimestamp);
        this.softly.assertThat(processedDeadLetters.toList())
                .hasSize(1);
        this.softly.assertThat(examples.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final ErrorExample value = producerRecord.value();
                    final ExampleDeadLetterWithContext example = value.getExample();
                    this.softly.assertThat(example.getDeadLetter()).isEqualTo(firstDeadLetter);
                    this.softly.assertThat(example.getKey()).isEqualTo("key");
                    this.softly.assertThat(example.getOffset()).isEqualTo(0L);
                    this.softly.assertThat(example.getPartition()).isEqualTo(0);
                    this.softly.assertThat(parseDateTime(example.getTimestamp()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                });

        final DeadLetter secondDeadLetter = DeadLetter.newBuilder()
                .setInputValue("foo")
                .setCause(ErrorDescription.newBuilder()
                        .setMessage("message")
                        .setStackTrace(StackTraceClassifierTest.STACK_TRACE)
                        .build())
                .setDescription("description")
                .build();
        final long secondTimestamp = 10L;
        input.add("key", secondDeadLetter, secondTimestamp);
        this.softly.assertThat(processedDeadLetters.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).isEqualTo("my-stream-dead-letter-topic+0+1");
                    final FullDeadLetterWithContext value = producerRecord.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("key");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getOffset()).isEqualTo(1L);
                    this.softly.assertThat(value.getDeadLetter()).isEqualTo(secondDeadLetter);
                    this.softly.assertThat(parseDateTime(value.getTimestamp()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(secondTimestamp)));
                });
        this.softly.assertThat(examples.toList()).isEmpty();
        this.assertNoDeadLetters();
    }

    private static String getDeserialized(final Headers headers, final String name) {
        try (final Deserializer<String> deserializer = new StringDeserializer()) {
            return deserializer.deserialize(null, headers.lastHeader(name).value());
        }
    }

    @Test
    void shouldProduceDeadLetterFromAvroDeadLetterAndAnalyze() {
        final TestInput<String, SpecificRecord> input = this.getStreamsInput(Serdes.String());
        final DeadLetter deadLetter = DeadLetter.newBuilder()
                .setInputValue("foo")
                .setCause(ErrorDescription.newBuilder()
                        .setMessage("message")
                        // null crashes the app
                        .setStackTrace(null)
                        .build())
                .setDescription("description")
                .setInputTimestamp(Instant.ofEpochMilli(200L))
                .build();

        final TestOutput<String, FullDeadLetterWithContext> processedDeadLetters =
                this.getProcessedDeadLetters();
        final TestOutput<String, FullErrorStatistics> statistics = this.getStatistics();
        final TestOutput<String, ErrorExample> examples = this.getExamples();

        input.add("key", deadLetter);
        final TestOutput<String, SpecificRecord> deadLetters = this.getDeadLetters();
        this.softly.assertThat(deadLetters.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).isEqualTo("key");
                    this.softly.assertThat(producerRecord.value()).isEqualTo(deadLetter);
                    this.softly.assertThat(
                                    getDeserialized(producerRecord.headers(), HEADER_ERRORS_PROCESSOR_NODE_ID_NAME))
                            .isEqualTo("analysis");
                });
        this.softly.assertThat(processedDeadLetters.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    //offset used by internal dead letter is 1. Maybe it has to do with exactly once guarantees
                    this.softly.assertThat(producerRecord.key()).isEqualTo("analyzer-stream-dead-letter-topic+0+1");
                    final FullDeadLetterWithContext value = producerRecord.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("key");
                    this.softly.assertThat(value.getTopic()).isEqualTo("analyzer-stream-dead-letter-topic");
                    this.softly.assertThat(value.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getOffset()).isEqualTo(1L);
                });
        this.softly.assertThat(statistics.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).startsWith("analyzer-stream-dead-letter-topic:");
                    final FullErrorStatistics value = producerRecord.value();
                    this.softly.assertThat(value.getCount()).isEqualTo(1);
                    this.softly.assertThat(value.getType()).isNotEmpty();
                    this.softly.assertThat(value.getTopic()).isEqualTo("analyzer-stream-dead-letter-topic");
                });
        this.softly.assertThat(examples.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).startsWith("analyzer-stream-dead-letter-topic:");
                    final ErrorExample value = producerRecord.value();
                    final ExampleDeadLetterWithContext example = value.getExample();
                    this.softly.assertThat(example.getKey()).isEqualTo("key");
                    this.softly.assertThat(example.getOffset()).isEqualTo(1L);
                    this.softly.assertThat(example.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getType()).isNotEmpty();
                    this.softly.assertThat(value.getTopic()).isEqualTo("analyzer-stream-dead-letter-topic");
                });
    }

    @Test
    void shouldProcessConnectErrors() {
        final TestInput<String, SpecificRecord> input = this.getConnectInput(Serdes.String());
        final TestOutput<String, FullDeadLetterWithContext> processedDeadLetters =
                this.getProcessedDeadLetters();
        final TestOutput<String, FullErrorStatistics> statistics = this.getStatistics();
        final TestOutput<String, ErrorExample> examples = this.getExamples();

        final long firstTimestamp = 0L;
        final Headers headers = new RecordHeaders()
                .add(ERROR_HEADER_ORIG_PARTITION, toBytes(1))
                .add(ERROR_HEADER_ORIG_TOPIC, toBytes("my-topic"))
                .add(ERROR_HEADER_ORIG_OFFSET, toBytes(10L))
                .add(ERROR_HEADER_STAGE, toBytes("VALUE_CONVERTER"))
                .add(ERROR_HEADER_EXECUTING_CLASS, toBytes("org.apache.kafka.connect.json.JsonConverter"))
                .add(ERROR_HEADER_EXCEPTION, toBytes("org.apache.kafka.connect.errors.DataException"))
                .add(ERROR_HEADER_TASK_ID, toBytes(2))
                .add(ERROR_HEADER_CONNECTOR_NAME, toBytes("my-connector"))
                .add(ERROR_HEADER_EXCEPTION_MESSAGE, toBytes("my message"))
                .add(ERROR_HEADER_EXCEPTION_STACK_TRACE, toBytes(StackTraceClassifierTest.STACK_TRACE));
        final TestRecord testRecord = TestRecord.newBuilder().setId(0).build();
        input.add("key", testRecord, 0L, headers);

        final DeadLetter expectedDeadLetter = DeadLetter.newBuilder()
                .setInputValue("{\"id\":0}")
                .setCause(ErrorDescription.newBuilder()
                        .setErrorClass("org.apache.kafka.connect.errors.DataException")
                        .setMessage("my message")
                        .setStackTrace(StackTraceClassifierTest.STACK_TRACE)
                        .build())
                .setDescription("Error in stage VALUE_CONVERTER (org.apache.kafka.connect.json.JsonConverter) in "
                        + "my-connector[2]")
                .setPartition(1)
                .setTopic("my-topic")
                .setOffset(10L)
                .setInputTimestamp(Instant.ofEpochMilli(firstTimestamp))
                .build();

        this.softly.assertThat(processedDeadLetters.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).isEqualTo("my-connect-dead-letter-topic+0+0");
                    final FullDeadLetterWithContext value = producerRecord.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("key");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-connect-dead-letter-topic");
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getOffset()).isEqualTo(0L);
                    this.softly.assertThat(value.getDeadLetter()).isEqualTo(expectedDeadLetter);
                    this.softly.assertThat(parseDateTime(value.getTimestamp()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                });
        this.softly.assertThat(statistics.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key())
                            .isEqualTo("my-connect-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final FullErrorStatistics value = producerRecord.value();
                    this.softly.assertThat(value.getCount()).isEqualTo(1);
                    this.softly.assertThat(parseDateTime(value.getCreated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(parseDateTime(value.getUpdated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-connect-dead-letter-topic");
                });
        this.softly.assertThat(examples.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key())
                            .isEqualTo("my-connect-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final ErrorExample value = producerRecord.value();
                    final ExampleDeadLetterWithContext example = value.getExample();
                    this.softly.assertThat(example.getDeadLetter()).isEqualTo(expectedDeadLetter);
                    this.softly.assertThat(example.getKey()).isEqualTo("key");
                    this.softly.assertThat(example.getOffset()).isEqualTo(0L);
                    this.softly.assertThat(example.getPartition()).isEqualTo(0);
                    this.softly.assertThat(parseDateTime(example.getTimestamp()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-connect-dead-letter-topic");
                });
        this.assertNoDeadLetters();
    }

    @Test
    void shouldProduceDeadLetterFromConnectDeadLetterAndAnalyze() {
        final TestInput<String, SpecificRecord> input = this.getStreamsInput(Serdes.String());
        // ERROR_HEADER_STAGE is missing and crashes the app
        final Headers headers = new RecordHeaders()
                .add(ERROR_HEADER_ORIG_PARTITION, toBytes(1))
                .add(ERROR_HEADER_ORIG_TOPIC, toBytes("my-topic"))
                .add(ERROR_HEADER_ORIG_OFFSET, toBytes(10L))
                .add(ERROR_HEADER_EXECUTING_CLASS, toBytes("org.apache.kafka.connect.json.JsonConverter"))
                .add(ERROR_HEADER_EXCEPTION, toBytes("org.apache.kafka.connect.errors.DataException"))
                .add(ERROR_HEADER_TASK_ID, toBytes(2))
                .add(ERROR_HEADER_CONNECTOR_NAME, toBytes("my-connector"))
                .add(ERROR_HEADER_EXCEPTION_MESSAGE, toBytes("my message"))
                .add(ERROR_HEADER_EXCEPTION_STACK_TRACE, toBytes(StackTraceClassifierTest.STACK_TRACE));
        final TestRecord testRecord = TestRecord.newBuilder().setId(0).build();

        final TestOutput<String, FullDeadLetterWithContext> processedDeadLetters =
                this.getProcessedDeadLetters();
        final TestOutput<String, FullErrorStatistics> statistics = this.getStatistics();
        final TestOutput<String, ErrorExample> examples = this.getExamples();

        input.add("key", testRecord, headers);
        final TestOutput<String, SpecificRecord> deadLetters = this.getDeadLetters();
        this.softly.assertThat(deadLetters.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).isEqualTo("key");
                    this.softly.assertThat(producerRecord.value()).isEqualTo(testRecord);
                });
        this.softly.assertThat(processedDeadLetters.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    //offset used by internal dead letter is 1. Maybe it has to do with exactly once guarantees
                    this.softly.assertThat(producerRecord.key()).isEqualTo("analyzer-stream-dead-letter-topic+0+1");
                    final FullDeadLetterWithContext value = producerRecord.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("key");
                    this.softly.assertThat(value.getTopic()).isEqualTo("analyzer-stream-dead-letter-topic");
                    this.softly.assertThat(value.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getOffset()).isEqualTo(1L);
                });
        this.softly.assertThat(statistics.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).startsWith("analyzer-stream-dead-letter-topic:");
                    final FullErrorStatistics value = producerRecord.value();
                    this.softly.assertThat(value.getCount()).isEqualTo(1);
                    this.softly.assertThat(value.getType()).isNotEmpty();
                    this.softly.assertThat(value.getTopic()).isEqualTo("analyzer-stream-dead-letter-topic");
                });
        this.softly.assertThat(examples.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).startsWith("analyzer-stream-dead-letter-topic:");
                    final ErrorExample value = producerRecord.value();
                    final ExampleDeadLetterWithContext example = value.getExample();
                    this.softly.assertThat(example.getKey()).isEqualTo("key");
                    this.softly.assertThat(example.getOffset()).isEqualTo(1L);
                    this.softly.assertThat(example.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getType()).isNotEmpty();
                    this.softly.assertThat(value.getTopic()).isEqualTo("analyzer-stream-dead-letter-topic");
                });
    }

    @Test
    void shouldProcessStreamsHeaderErrors() {
        final TestInput<String, String> input = this.getStreamsInput(Serdes.String())
                .withValueSerde(Serdes.String());
        final TestOutput<String, FullDeadLetterWithContext> processedDeadLetters =
                this.getProcessedDeadLetters();
        final TestOutput<String, FullErrorStatistics> statistics = this.getStatistics();
        final TestOutput<String, ErrorExample> examples = this.getExamples();

        final long firstTimestamp = 0L;
        final Headers headers = new RecordHeaders()
                .add(PARTITION, toBytes(1))
                .add(TOPIC, toBytes("my-topic"))
                .add(OFFSET, toBytes(10L))
                .add(DESCRIPTION, toBytes("description"))
                .add(EXCEPTION_CLASS_NAME, toBytes("org.apache.kafka.connect.errors.DataException"))
                .add(EXCEPTION_MESSAGE, toBytes("my message"))
                .add(EXCEPTION_STACK_TRACE, toBytes(StackTraceClassifierTest.STACK_TRACE));

        input.add("key", "value", firstTimestamp, headers);

        final DeadLetter expectedDeadLetter = DeadLetter.newBuilder()
                .setInputValue("value")
                .setCause(ErrorDescription.newBuilder()
                        .setErrorClass("org.apache.kafka.connect.errors.DataException")
                        .setMessage("my message")
                        .setStackTrace(StackTraceClassifierTest.STACK_TRACE)
                        .build())
                .setDescription("description")
                .setPartition(1)
                .setTopic("my-topic")
                .setOffset(10L)
                .setInputTimestamp(Instant.ofEpochMilli(firstTimestamp))
                .build();

        this.softly.assertThat(processedDeadLetters.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).isEqualTo("my-stream-dead-letter-topic+0+0");
                    final FullDeadLetterWithContext value = producerRecord.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("key");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getOffset()).isEqualTo(0L);
                    this.softly.assertThat(value.getDeadLetter()).isEqualTo(expectedDeadLetter);
                    this.softly.assertThat(parseDateTime(value.getTimestamp()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                });
        this.softly.assertThat(statistics.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final FullErrorStatistics value = producerRecord.value();
                    this.softly.assertThat(value.getCount()).isEqualTo(1);
                    this.softly.assertThat(parseDateTime(value.getCreated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(parseDateTime(value.getUpdated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                });
        this.softly.assertThat(examples.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final ErrorExample value = producerRecord.value();
                    final ExampleDeadLetterWithContext example = value.getExample();
                    this.softly.assertThat(example.getDeadLetter()).isEqualTo(expectedDeadLetter);
                    this.softly.assertThat(example.getKey()).isEqualTo("key");
                    this.softly.assertThat(example.getOffset()).isEqualTo(0L);
                    this.softly.assertThat(example.getPartition()).isEqualTo(0);
                    this.softly.assertThat(parseDateTime(example.getTimestamp()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                });
        this.assertNoDeadLetters();
    }

    @Test
    void shouldProduceDeadLetterFromStreamsHeaderErrorAndAnalyze() {
        final TestInput<String, SpecificRecord> input = this.getStreamsInput(Serdes.String());
        // EXCEPTION_STACK_TRACE is missing and crashes the app
        final Headers headers = new RecordHeaders()
                .add(PARTITION, toBytes(1))
                .add(TOPIC, toBytes("my-topic"))
                .add(OFFSET, toBytes(10L))
                .add(DESCRIPTION, toBytes("description"))
                .add(EXCEPTION_CLASS_NAME, toBytes("org.apache.kafka.connect.errors.DataException"))
                .add(EXCEPTION_MESSAGE, toBytes("my message"));
        final TestRecord testRecord = TestRecord.newBuilder().setId(0).build();

        final TestOutput<String, FullDeadLetterWithContext> processedDeadLetters =
                this.getProcessedDeadLetters();
        final TestOutput<String, FullErrorStatistics> statistics = this.getStatistics();
        final TestOutput<String, ErrorExample> examples = this.getExamples();

        input.add("key", testRecord, headers);
        final TestOutput<String, SpecificRecord> deadLetters = this.getDeadLetters();
        this.softly.assertThat(deadLetters.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).isEqualTo("key");
                    this.softly.assertThat(producerRecord.value()).isEqualTo(testRecord);
                });
        this.softly.assertThat(processedDeadLetters.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    //offset used by internal dead letter is 1. Maybe it has to do with exactly once guarantees
                    this.softly.assertThat(producerRecord.key()).isEqualTo("analyzer-stream-dead-letter-topic+0+1");
                    final FullDeadLetterWithContext value = producerRecord.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("key");
                    this.softly.assertThat(value.getTopic()).isEqualTo("analyzer-stream-dead-letter-topic");
                    this.softly.assertThat(value.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getOffset()).isEqualTo(1L);
                });
        this.softly.assertThat(statistics.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).startsWith("analyzer-stream-dead-letter-topic:");
                    final FullErrorStatistics value = producerRecord.value();
                    this.softly.assertThat(value.getCount()).isEqualTo(1);
                    this.softly.assertThat(value.getType()).isNotEmpty();
                    this.softly.assertThat(value.getTopic()).isEqualTo("analyzer-stream-dead-letter-topic");
                });
        this.softly.assertThat(examples.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).startsWith("analyzer-stream-dead-letter-topic:");
                    final ErrorExample value = producerRecord.value();
                    final ExampleDeadLetterWithContext example = value.getExample();
                    this.softly.assertThat(example.getKey()).isEqualTo("key");
                    this.softly.assertThat(example.getOffset()).isEqualTo(1L);
                    this.softly.assertThat(example.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getType()).isNotEmpty();
                    this.softly.assertThat(value.getTopic()).isEqualTo("analyzer-stream-dead-letter-topic");
                });
    }

    @Test
    void shouldProcessNativeStreamsErrors() {
        final TestInput<String, String> input = this.getStreamsInput(Serdes.String())
                .withValueSerde(Serdes.String());
        final TestOutput<String, FullDeadLetterWithContext> processedDeadLetters =
                this.getProcessedDeadLetters();
        final TestOutput<String, FullErrorStatistics> statistics = this.getStatistics();
        final TestOutput<String, ErrorExample> examples = this.getExamples();

        final long firstTimestamp = 0L;
        final Headers headers = new RecordHeaders()
                .add(HEADER_ERRORS_PARTITION_NAME, toBytes(1))
                .add(HEADER_ERRORS_TOPIC_NAME, toBytes("my-topic"))
                .add(HEADER_ERRORS_OFFSET_NAME, toBytes(10L))
                .add(HEADER_ERRORS_PROCESSOR_NODE_ID_NAME, toBytes("processor"))
                .add(HEADER_ERRORS_TASK_ID_NAME, toBytes("task"))
                .add(HEADER_ERRORS_EXCEPTION_NAME, toBytes("org.apache.kafka.connect.errors.DataException"))
                .add(HEADER_ERRORS_EXCEPTION_MESSAGE_NAME, toBytes("my message"))
                .add(HEADER_ERRORS_STACKTRACE_NAME, toBytes(StackTraceClassifierTest.STACK_TRACE));

        input.add("key", "value", firstTimestamp, headers);

        final DeadLetter expectedDeadLetter = DeadLetter.newBuilder()
                .setInputValue("value")
                .setCause(ErrorDescription.newBuilder()
                        .setErrorClass("org.apache.kafka.connect.errors.DataException")
                        .setMessage("my message")
                        .setStackTrace(StackTraceClassifierTest.STACK_TRACE)
                        .build())
                .setDescription("Error in processor node processor in task task")
                .setPartition(1)
                .setTopic("my-topic")
                .setOffset(10L)
                .setInputTimestamp(Instant.ofEpochMilli(firstTimestamp))
                .build();

        this.softly.assertThat(processedDeadLetters.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).isEqualTo("my-stream-dead-letter-topic+0+0");
                    final FullDeadLetterWithContext value = producerRecord.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("key");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getOffset()).isEqualTo(0L);
                    this.softly.assertThat(value.getDeadLetter()).isEqualTo(expectedDeadLetter);
                    this.softly.assertThat(parseDateTime(value.getTimestamp()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                });
        this.softly.assertThat(statistics.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final FullErrorStatistics value = producerRecord.value();
                    this.softly.assertThat(value.getCount()).isEqualTo(1);
                    this.softly.assertThat(parseDateTime(value.getCreated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(parseDateTime(value.getUpdated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                });
        this.softly.assertThat(examples.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final ErrorExample value = producerRecord.value();
                    final ExampleDeadLetterWithContext example = value.getExample();
                    this.softly.assertThat(example.getDeadLetter()).isEqualTo(expectedDeadLetter);
                    this.softly.assertThat(example.getKey()).isEqualTo("key");
                    this.softly.assertThat(example.getOffset()).isEqualTo(0L);
                    this.softly.assertThat(example.getPartition()).isEqualTo(0);
                    this.softly.assertThat(parseDateTime(example.getTimestamp()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                });
        this.assertNoDeadLetters();
    }

    @Test
    void shouldProduceDeadLetterFromNativeStreamsErrorAndAnalyze() {
        final TestInput<String, SpecificRecord> input = this.getStreamsInput(Serdes.String());
        // HEADER_ERRORS_STACKTRACE_NAME is missing and crashes the app
        final Headers headers = new RecordHeaders()
                .add(HEADER_ERRORS_PARTITION_NAME, toBytes(1))
                .add(HEADER_ERRORS_TOPIC_NAME, toBytes("my-topic"))
                .add(HEADER_ERRORS_OFFSET_NAME, toBytes(10L))
                .add(HEADER_ERRORS_PROCESSOR_NODE_ID_NAME, toBytes("processor"))
                .add(HEADER_ERRORS_TASK_ID_NAME, toBytes("task"))
                .add(HEADER_ERRORS_EXCEPTION_NAME, toBytes("org.apache.kafka.connect.errors.DataException"))
                .add(HEADER_ERRORS_EXCEPTION_MESSAGE_NAME, toBytes("my message"));
        final TestRecord testRecord = TestRecord.newBuilder().setId(0).build();

        final TestOutput<String, FullDeadLetterWithContext> processedDeadLetters =
                this.getProcessedDeadLetters();
        final TestOutput<String, FullErrorStatistics> statistics = this.getStatistics();
        final TestOutput<String, ErrorExample> examples = this.getExamples();

        input.add("key", testRecord, headers);
        final TestOutput<String, SpecificRecord> deadLetters = this.getDeadLetters();
        this.softly.assertThat(deadLetters.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).isEqualTo("key");
                    this.softly.assertThat(producerRecord.value()).isEqualTo(testRecord);
                });
        this.softly.assertThat(processedDeadLetters.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    //offset used by internal dead letter is 1. Maybe it has to do with exactly once guarantees
                    this.softly.assertThat(producerRecord.key()).isEqualTo("analyzer-stream-dead-letter-topic+0+1");
                    final FullDeadLetterWithContext value = producerRecord.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("key");
                    this.softly.assertThat(value.getTopic()).isEqualTo("analyzer-stream-dead-letter-topic");
                    this.softly.assertThat(value.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getOffset()).isEqualTo(1L);
                });
        this.softly.assertThat(statistics.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).startsWith("analyzer-stream-dead-letter-topic:");
                    final FullErrorStatistics value = producerRecord.value();
                    this.softly.assertThat(value.getCount()).isEqualTo(1);
                    this.softly.assertThat(value.getType()).isNotEmpty();
                    this.softly.assertThat(value.getTopic()).isEqualTo("analyzer-stream-dead-letter-topic");
                });
        this.softly.assertThat(examples.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).startsWith("analyzer-stream-dead-letter-topic:");
                    final ErrorExample value = producerRecord.value();
                    final ExampleDeadLetterWithContext example = value.getExample();
                    this.softly.assertThat(example.getKey()).isEqualTo("key");
                    this.softly.assertThat(example.getOffset()).isEqualTo(1L);
                    this.softly.assertThat(example.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getType()).isNotEmpty();
                    this.softly.assertThat(value.getTopic()).isEqualTo("analyzer-stream-dead-letter-topic");
                });
    }

    @Test
    void shouldReadAvroKey() {
        final TestInput<SpecificRecord, SpecificRecord> input =
                this.getStreamsInput(DeadLetterAnalyzerTopology.getSpecificAvroSerde());
        final TestOutput<String, FullDeadLetterWithContext> processedDeadLetters =
                this.getProcessedDeadLetters();
        final TestOutput<String, FullErrorStatistics> statistics = this.getStatistics();
        final TestOutput<String, ErrorExample> examples = this.getExamples();

        final DeadLetter firstDeadLetter = DeadLetter.newBuilder()
                .setInputValue("foo")
                .setCause(ErrorDescription.newBuilder()
                        .setMessage("message")
                        .setStackTrace(StackTraceClassifierTest.STACK_TRACE)
                        .build())
                .setDescription("description")
                .setInputTimestamp(Instant.ofEpochMilli(200L))
                .build();
        input.add(TestRecord.newBuilder().setId(1).build(), firstDeadLetter);
        this.softly.assertThat(processedDeadLetters.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).isEqualTo("my-stream-dead-letter-topic+0+0");
                    final FullDeadLetterWithContext value = producerRecord.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("{\"id\":1}");
                });
        this.softly.assertThat(statistics.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> this.softly.assertThat(producerRecord.key())
                        .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)"));
        this.softly.assertThat(examples.toList())
                .hasSize(1)
                .anySatisfy(producerRecord -> {
                    this.softly.assertThat(producerRecord.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final ErrorExample value = producerRecord.value();
                    final ExampleDeadLetterWithContext example = value.getExample();
                    this.softly.assertThat(example.getKey()).isEqualTo("{\"id\":1}");
                });
        this.assertNoDeadLetters();
    }

    private void assertNoDeadLetters() {
        final TestOutput<String, SpecificRecord> deadLetters = this.getDeadLetters();
        this.softly.assertThat(deadLetters.toList()).isEmpty();
    }

    private TestOutput<String, FullDeadLetterWithContext> getProcessedDeadLetters() {
        final Preconfigured<Serde<FullDeadLetterWithContext>> valueSerde = getLargeMessageSerde();
        return this.topology.streamOutput(TOPIC_CONFIG.getOutputTopic())
                .configureWithValueSerde(valueSerde);
    }

    private static <T> Preconfigured<Serde<T>> getLargeMessageSerde() {
        final Serde<T> valueSerde = new LargeMessageSerde<>();
        return Preconfigured.create(valueSerde);
    }

    private TestOutput<String, FullErrorStatistics> getStatistics() {
        return this.topology.streamOutput(DeadLetterAnalyzerTopology.getStatsTopic(TOPIC_CONFIG))
                .configureWithValueSerde(DeadLetterAnalyzerTopology.getSpecificAvroSerde());
    }

    private TestOutput<String, ErrorExample> getExamples() {
        final Preconfigured<Serde<ErrorExample>> valueSerde = getLargeMessageSerde();
        return this.topology.streamOutput(DeadLetterAnalyzerTopology.getExamplesTopic(TOPIC_CONFIG))
                .configureWithValueSerde(valueSerde);
    }

    private TestOutput<String, SpecificRecord> getDeadLetters() {
        return this.topology.streamOutput(TOPIC_CONFIG.getErrorTopic())
                .configureWithValueSerde(DeadLetterAnalyzerTopology.getSpecificAvroSerde());
    }

    private <K> TestInput<K, SpecificRecord> getStreamsInput(final Serde<K> keySerde) {
        return this.getStreamsInput(Preconfigured.create(keySerde));
    }

    private <K> TestInput<K, SpecificRecord> getStreamsInput(final Preconfigured<? extends Serde<K>> keySerde) {
        return this.getInput(keySerde, "my-stream-dead-letter-topic");
    }

    private <K> TestInput<K, SpecificRecord> getInput(final Preconfigured<? extends Serde<K>> keySerde,
            final String topic) {
        return this.topology.input(topic)
                .configureWithValueSerde(DeadLetterAnalyzerTopology.getSpecificAvroSerde())
                .configureWithKeySerde(keySerde);
    }

    private <K> TestInput<K, SpecificRecord> getConnectInput(final Serde<K> keySerde) {
        return this.getInput(Preconfigured.create(keySerde), "my-connect-dead-letter-topic");
    }
}
