/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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
import static org.jooq.lambda.Seq.seq;

import com.bakdata.fluent_kafka_streams_tests.TestInput;
import com.bakdata.fluent_kafka_streams_tests.TestOutput;
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(SoftAssertionsExtension.class)
class DeadLetterAnalyzerTopologyTest {
    private DeadLetterAnalyzerTopology app;
    @RegisterExtension
    TestTopologyExtension<String, DeadLetter> topology = this.createTestTopology();
    @InjectSoftAssertions
    private SoftAssertions softly;

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
        this.softly.assertThat(seq(processedDeadLetters).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key()).isEqualTo("my-stream-dead-letter-topic+0+0");
                    final FullDeadLetterWithContext value = record.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("key");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getOffset()).isEqualTo(0L);
                    this.softly.assertThat(value.getDeadLetter()).isEqualTo(deadLetter);
                    this.softly.assertThat(parseDateTime(value.getTimestamp()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(timestamp)));
                });
        this.softly.assertThat(seq(statistics).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final FullErrorStatistics value = record.value();
                    this.softly.assertThat(value.getCount()).isEqualTo(1);
                    this.softly.assertThat(parseDateTime(value.getCreated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(timestamp)));
                    this.softly.assertThat(parseDateTime(value.getUpdated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(timestamp)));
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                });
        this.softly.assertThat(seq(examples).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final ErrorExample value = record.value();
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
        this.softly.assertThat(seq(statistics).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final FullErrorStatistics value = record.value();
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
        this.softly.assertThat(seq(statistics).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final FullErrorStatistics value = record.value();
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
        this.softly.assertThat(seq(processedDeadLetters).toList())
                .hasSize(1);
        this.softly.assertThat(seq(examples).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final ErrorExample value = record.value();
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
        this.softly.assertThat(seq(processedDeadLetters).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key()).isEqualTo("my-stream-dead-letter-topic+0+1");
                    final FullDeadLetterWithContext value = record.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("key");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getOffset()).isEqualTo(1L);
                    this.softly.assertThat(value.getDeadLetter()).isEqualTo(secondDeadLetter);
                    this.softly.assertThat(parseDateTime(value.getTimestamp()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(secondTimestamp)));
                });
        this.softly.assertThat(seq(examples).toList()).isEmpty();
        this.assertNoDeadLetters();
    }

    @Test
    void shouldProduceDeadLetterAndAnalyze() {
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
        final Seq<ProducerRecord<String, DeadLetter>> deadLetters = this.getDeadLetters();
        this.softly.assertThat(deadLetters.toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key()).isEqualTo("key");
                    final DeadLetter value = record.value();
                    this.softly.assertThat(value.getDescription()).isEqualTo("Error analyzing dead letter");
                    this.softly.assertThat(value.getTopic()).hasValue("my-stream-dead-letter-topic");
                    this.softly.assertThat(value.getPartition()).hasValue(0);
                    this.softly.assertThat(value.getOffset()).hasValue(0L);
                });
        this.softly.assertThat(seq(processedDeadLetters).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    //offset used by internal dead letter is 1. Maybe it has to do with exactly once guarantees
                    this.softly.assertThat(record.key()).isEqualTo("analyzer-stream-dead-letter-topic+0+1");
                    final FullDeadLetterWithContext value = record.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("key");
                    this.softly.assertThat(value.getTopic()).isEqualTo("analyzer-stream-dead-letter-topic");
                    this.softly.assertThat(value.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getOffset()).isEqualTo(1L);
                });
        this.softly.assertThat(seq(statistics).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key()).startsWith("analyzer-stream-dead-letter-topic:");
                    final FullErrorStatistics value = record.value();
                    this.softly.assertThat(value.getCount()).isEqualTo(1);
                    this.softly.assertThat(value.getType()).isNotEmpty();
                    this.softly.assertThat(value.getTopic()).isEqualTo("analyzer-stream-dead-letter-topic");
                });
        this.softly.assertThat(seq(examples).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key()).startsWith("analyzer-stream-dead-letter-topic:");
                    final ErrorExample value = record.value();
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

        this.softly.assertThat(seq(processedDeadLetters).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key()).isEqualTo("my-connect-dead-letter-topic+0+0");
                    final FullDeadLetterWithContext value = record.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("key");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-connect-dead-letter-topic");
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getOffset()).isEqualTo(0L);
                    this.softly.assertThat(value.getDeadLetter()).isEqualTo(expectedDeadLetter);
                    this.softly.assertThat(parseDateTime(value.getTimestamp()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                });
        this.softly.assertThat(seq(statistics).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key())
                            .isEqualTo("my-connect-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final FullErrorStatistics value = record.value();
                    this.softly.assertThat(value.getCount()).isEqualTo(1);
                    this.softly.assertThat(parseDateTime(value.getCreated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(parseDateTime(value.getUpdated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-connect-dead-letter-topic");
                });
        this.softly.assertThat(seq(examples).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key())
                            .isEqualTo("my-connect-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final ErrorExample value = record.value();
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

        this.softly.assertThat(seq(processedDeadLetters).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key()).isEqualTo("my-stream-dead-letter-topic+0+0");
                    final FullDeadLetterWithContext value = record.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("key");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getPartition()).isEqualTo(0);
                    this.softly.assertThat(value.getOffset()).isEqualTo(0L);
                    this.softly.assertThat(value.getDeadLetter()).isEqualTo(expectedDeadLetter);
                    this.softly.assertThat(parseDateTime(value.getTimestamp()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                });
        this.softly.assertThat(seq(statistics).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final FullErrorStatistics value = record.value();
                    this.softly.assertThat(value.getCount()).isEqualTo(1);
                    this.softly.assertThat(parseDateTime(value.getCreated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(parseDateTime(value.getUpdated()))
                            .isEqualToIgnoringNanos(toDateTime(Instant.ofEpochMilli(firstTimestamp)));
                    this.softly.assertThat(value.getType()).isEqualTo("org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    this.softly.assertThat(value.getTopic()).isEqualTo("my-stream-dead-letter-topic");
                });
        this.softly.assertThat(seq(examples).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final ErrorExample value = record.value();
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
    void shouldReadAvroKey() {
        final TestInput<SpecificRecord, SpecificRecord> input =
                this.getStreamsInput(this.app.configureForKeys(DeadLetterAnalyzerTopology.getSpecificAvroSerde()));
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
        this.softly.assertThat(seq(processedDeadLetters).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key()).isEqualTo("my-stream-dead-letter-topic+0+0");
                    final FullDeadLetterWithContext value = record.value();
                    this.softly.assertThat(value.getKey()).isEqualTo("{\"id\":1}");
                });
        this.softly.assertThat(seq(statistics).toList())
                .hasSize(1)
                .anySatisfy(record -> this.softly.assertThat(record.key())
                        .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)"));
        this.softly.assertThat(seq(examples).toList())
                .hasSize(1)
                .anySatisfy(record -> {
                    this.softly.assertThat(record.key())
                            .isEqualTo("my-stream-dead-letter-topic:org.jdbi.v3.core.Jdbi.open(Jdbi.java:319)");
                    final ErrorExample value = record.value();
                    final ExampleDeadLetterWithContext example = value.getExample();
                    this.softly.assertThat(example.getKey()).isEqualTo("{\"id\":1}");
                });
        this.assertNoDeadLetters();
    }

    private void assertNoDeadLetters() {
        final Iterable<ProducerRecord<String, DeadLetter>> deadLetters = this.getDeadLetters();
        this.softly.assertThat(seq(deadLetters).toList()).isEmpty();
    }

    private TestTopologyExtension<String, DeadLetter> createTestTopology() {
        final StreamsApp app = new DeadLetterAnalyzerApplication();
        final StreamsTopicConfig topicConfig = StreamsTopicConfig.builder()
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
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(app, new AppConfiguration<>(topicConfig));
        return new TestTopologyExtension<>(properties -> this.createTopology(properties, configuredApp),
                TestTopologyFactory.getKafkaPropertiesWithSchemaRegistryUrl(configuredApp));
    }

    private Topology createTopology(final Map<String, Object> properties, final ConfiguredStreamsApp<?> configuredApp) {
        final TopologyBuilder builder = new TopologyBuilder(configuredApp.getTopics(), properties);
        this.app = new DeadLetterAnalyzerTopology(builder);
        this.app.buildTopology();
        return builder.build();
    }

    private TestOutput<String, FullDeadLetterWithContext> getProcessedDeadLetters() {
        final Serde<FullDeadLetterWithContext> valueSerde = this.getLargeMessageSerde();
        return this.topology.streamOutput(this.app.getOutputTopic())
                .withValueSerde(valueSerde);
    }

    private <T> Serde<T> getLargeMessageSerde() {
        final Serde<T> valueSerde = new LargeMessageSerde<>();
        return this.app.configureForValues(Preconfigured.create(valueSerde));
    }

    private TestOutput<String, FullErrorStatistics> getStatistics() {
        return this.topology.streamOutput(this.app.getStatsTopic())
                .withValueSerde(new Configurator(this.topology.getProperties()).configureForValues(
                        DeadLetterAnalyzerTopology.getSpecificAvroSerde()));
    }

    private TestOutput<String, ErrorExample> getExamples() {
        final Serde<ErrorExample> valueSerde = this.getLargeMessageSerde();
        return this.topology.streamOutput(this.app.getExamplesTopic())
                .withValueSerde(valueSerde);
    }

    private Seq<ProducerRecord<String, DeadLetter>> getDeadLetters() {
        final Serde<DeadLetter> valueSerde = this.getLargeMessageSerde();
        final TestOutput<String, byte[]> output = this.topology.streamOutput(this.app.getErrorTopic())
                .withValueSerde(Serdes.ByteArray());
        return seq(output)
                // Record has already been consumed by the analyzer and headers are modified.
                // This only happens in the TestDriver because record headers are reused.
                // In Kafka this does not happen because the headers are stored server-side.
                .map(record -> deserializeNonBacked(valueSerde, record));
    }

    private <K> TestInput<K, SpecificRecord> getStreamsInput(final Serde<K> keySerde) {
        return this.getInput(keySerde, "my-stream-dead-letter-topic");
    }

    private <K> TestInput<K, SpecificRecord> getInput(final Serde<K> keySerde, final String topic) {
        return this.topology.input(topic)
                .withValueSerde(this.app.configureForValues(DeadLetterAnalyzerTopology.getSpecificAvroSerde()))
                .withKeySerde(keySerde);
    }

    private <K> TestInput<K, SpecificRecord> getConnectInput(final Serde<K> keySerde) {
        return this.getInput(keySerde, "my-connect-dead-letter-topic");
    }
}
