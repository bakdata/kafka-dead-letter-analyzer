/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata
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
import static com.bakdata.kafka.ErrorHeaderProcessor.DESCRIPTION;
import static com.bakdata.kafka.ErrorHeaderProcessor.EXCEPTION_CLASS_NAME;
import static com.bakdata.kafka.ErrorHeaderProcessor.EXCEPTION_MESSAGE;
import static com.bakdata.kafka.ErrorHeaderProcessor.EXCEPTION_STACK_TRACE;
import static com.bakdata.kafka.ErrorHeaderProcessor.INPUT_TIMESTAMP;
import static com.bakdata.kafka.ErrorHeaderProcessor.OFFSET;
import static com.bakdata.kafka.ErrorHeaderProcessor.PARTITION;
import static com.bakdata.kafka.ErrorHeaderProcessor.TOPIC;
import static com.bakdata.kafka.StreamsDeadLetterParser.FAULTY_OFFSET_HEADER;

import java.time.Instant;
import java.util.stream.Stream;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
class StreamsDeadLetterParserTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    static Stream<Arguments> generateMissingRequiredHeaders() {
        return Stream.of(
                Arguments.of(new RecordHeaders()
                                .add(TOPIC, toBytes("my-topic"))
                                .add(OFFSET, toBytes(10L))
                                .add(DESCRIPTION, toBytes("description"))
                                .add(EXCEPTION_CLASS_NAME, toBytes("org.apache.kafka.connect.errors.DataException"))
                                .add(EXCEPTION_MESSAGE, toBytes("my message"))
                                .add(EXCEPTION_STACK_TRACE, toBytes(StackTraceClassifierTest.STACK_TRACE)),
                        String.format("Missing required header %s", PARTITION)
                ),
                Arguments.of(new RecordHeaders()
                                .add(PARTITION, toBytes(1))
                                .add(OFFSET, toBytes(10L))
                                .add(DESCRIPTION, toBytes("description"))
                                .add(EXCEPTION_CLASS_NAME, toBytes("org.apache.kafka.connect.errors.DataException"))
                                .add(EXCEPTION_MESSAGE, toBytes("my message"))
                                .add(EXCEPTION_STACK_TRACE, toBytes(StackTraceClassifierTest.STACK_TRACE)),
                        String.format("Missing required header %s", TOPIC)
                ),
                Arguments.of(new RecordHeaders()
                                .add(PARTITION, toBytes(1))
                                .add(TOPIC, toBytes("my-topic"))
                                .add(DESCRIPTION, toBytes("description"))
                                .add(EXCEPTION_CLASS_NAME, toBytes("org.apache.kafka.connect.errors.DataException"))
                                .add(EXCEPTION_MESSAGE, toBytes("my message"))
                                .add(EXCEPTION_STACK_TRACE, toBytes(StackTraceClassifierTest.STACK_TRACE)),
                        String.format("Missing required header %s", OFFSET)
                ),
                Arguments.of(new RecordHeaders()
                                .add(PARTITION, toBytes(1))
                                .add(TOPIC, toBytes("my-topic"))
                                .add(OFFSET, toBytes(10L))
                                .add(EXCEPTION_CLASS_NAME, toBytes("org.apache.kafka.connect.errors.DataException"))
                                .add(EXCEPTION_MESSAGE, toBytes("my message"))
                                .add(EXCEPTION_STACK_TRACE, toBytes(StackTraceClassifierTest.STACK_TRACE)),
                        String.format("Missing required header %s", DESCRIPTION)
                ),
                Arguments.of(new RecordHeaders()
                                .add(PARTITION, toBytes(1))
                                .add(TOPIC, toBytes("my-topic"))
                                .add(OFFSET, toBytes(10L))
                                .add(DESCRIPTION, toBytes("description"))
                                .add(EXCEPTION_MESSAGE, toBytes("my message"))
                                .add(EXCEPTION_STACK_TRACE, toBytes(StackTraceClassifierTest.STACK_TRACE)),
                        String.format("Missing required header %s", EXCEPTION_CLASS_NAME)
                ),
                Arguments.of(new RecordHeaders()
                                .add(PARTITION, toBytes(1))
                                .add(TOPIC, toBytes("my-topic"))
                                .add(OFFSET, toBytes(10L))
                                .add(DESCRIPTION, toBytes("description"))
                                .add(EXCEPTION_CLASS_NAME, toBytes("org.apache.kafka.connect.errors.DataException"))
                                .add(EXCEPTION_STACK_TRACE, toBytes(StackTraceClassifierTest.STACK_TRACE)),
                        String.format("Missing required header %s", EXCEPTION_MESSAGE)
                ),
                Arguments.of(new RecordHeaders()
                                .add(PARTITION, toBytes(1))
                                .add(TOPIC, toBytes("my-topic"))
                                .add(OFFSET, toBytes(10L))
                                .add(DESCRIPTION, toBytes("description"))
                                .add(EXCEPTION_CLASS_NAME, toBytes("org.apache.kafka.connect.errors.DataException"))
                                .add(EXCEPTION_MESSAGE, toBytes("my message")),
                        String.format("Missing required header %s", EXCEPTION_STACK_TRACE)
                )
        );
    }

    static Stream<Arguments> generateNonNullableHeaders() {
        return Stream.of(
                Arguments.of(generateDefaultHeaders()
                        .add(PARTITION, null), "Cannot parse int from null"
                ),
                Arguments.of(generateDefaultHeaders()
                        .add(OFFSET, null), "Cannot parse long from null"
                )
        );
    }

    private static Headers generateDefaultHeaders() {
        return new RecordHeaders()
                .add(PARTITION, toBytes(1))
                .add(TOPIC, toBytes("my-topic"))
                .add(OFFSET, toBytes(10L))
                .add(DESCRIPTION, toBytes("description"))
                .add(EXCEPTION_CLASS_NAME, toBytes("org.apache.kafka.connect.errors.DataException"))
                .add(EXCEPTION_MESSAGE, toBytes("my message"))
                .add(EXCEPTION_STACK_TRACE, toBytes(StackTraceClassifierTest.STACK_TRACE))
                .add(INPUT_TIMESTAMP, toBytes(200L));
    }

    @Test
    void shouldConvert() {
        final Headers headers = generateDefaultHeaders();

        this.softly.assertThat(new StreamsDeadLetterParser().convert("foo", headers, 0))
                .satisfies(deadLetter -> {
                    this.softly.assertThat(deadLetter.getInputValue()).hasValue("foo");
                    this.softly.assertThat(deadLetter.getPartition()).hasValue(1);
                    this.softly.assertThat(deadLetter.getTopic()).hasValue("my-topic");
                    this.softly.assertThat(deadLetter.getOffset()).hasValue(10L);
                    final ErrorDescription cause = deadLetter.getCause();
                    this.softly.assertThat(cause.getErrorClass())
                            .hasValue("org.apache.kafka.connect.errors.DataException");
                    this.softly.assertThat(cause.getMessage()).hasValue("my message");
                    this.softly.assertThat(cause.getStackTrace()).hasValue(StackTraceClassifierTest.STACK_TRACE);
                    this.softly.assertThat(deadLetter.getDescription()).isEqualTo("description");
                    this.softly.assertThat(deadLetter.getInputTimestamp()).hasValue(Instant.ofEpochMilli(200L));
                });
    }

    @Test
    void shouldConvertFaultyOffsetHeader() {
        final Headers headers = generateDefaultHeaders()
                .remove(OFFSET)
                .add(FAULTY_OFFSET_HEADER, toBytes(100L));
        this.softly.assertThat(new StreamsDeadLetterParser().convert("foo", headers, 0))
                .satisfies(deadLetter -> this.softly.assertThat(deadLetter.getOffset()).hasValue(100L));
    }

    @Test
    void shouldIgnoreFaultyOffsetHeader() {
        final Headers headers = generateDefaultHeaders()
                .add(OFFSET, toBytes(10L))
                .add(FAULTY_OFFSET_HEADER, toBytes(100L));
        this.softly.assertThat(new StreamsDeadLetterParser().convert("foo", headers, 0))
                .satisfies(deadLetter -> this.softly.assertThat(deadLetter.getOffset()).hasValue(10L));
    }

    @Test
    void shouldConvertNullMessageHeader() {
        final Headers headers = generateDefaultHeaders()
                .add(EXCEPTION_MESSAGE, null);
        this.softly.assertThat(new StreamsDeadLetterParser().convert("foo", headers, 0))
                .satisfies(deadLetter -> this.softly.assertThat(deadLetter.getCause().getMessage()).isNotPresent());
    }

    @Test
    void shouldFallbackToRecordTimestamp() {
        Headers headers = generateDefaultHeaders().remove(INPUT_TIMESTAMP);
        this.softly.assertThat(new StreamsDeadLetterParser().convert("foo", headers, 500))
                .satisfies(deadLetter -> this.softly.assertThat(deadLetter.getInputTimestamp())
                        .hasValue(Instant.ofEpochMilli(500L)));
    }


    @ParameterizedTest
    @MethodSource("generateMissingRequiredHeaders")
    void shouldThrowWithMissingRequiredHeaders(final Headers headers, final String message) {
        this.softly.assertThatThrownBy(() -> new StreamsDeadLetterParser().convert("foo", headers, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
    }

    @ParameterizedTest
    @MethodSource("generateNonNullableHeaders")
    void shouldThrowWithNonNullableHeaders(final Headers headers, final String message) {
        this.softly.assertThatThrownBy(() -> new StreamsDeadLetterParser().convert("foo", headers, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
    }

}
