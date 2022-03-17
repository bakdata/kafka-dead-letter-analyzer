/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
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

import java.nio.charset.StandardCharsets;
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
class ConnectDeadLetterConverterTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    static byte[] toBytes(final int value) {
        return toBytes(String.valueOf(value));
    }

    static byte[] toBytes(final long value) {
        return toBytes(String.valueOf(value));
    }

    static byte[] toBytes(final String value) {
        if (value != null) {
            return value.getBytes(StandardCharsets.UTF_8);
        } else {
            return null;
        }
    }

    static Stream<Arguments> generateMissingRequiredHeaders() {
        return Stream.of(
                Arguments.of(new RecordHeaders()
                                .add(ERROR_HEADER_EXECUTING_CLASS, toBytes("org.apache.kafka.connect.json"
                                        + ".JsonConverter"))
                                .add(ERROR_HEADER_TASK_ID, toBytes(2))
                                .add(ERROR_HEADER_CONNECTOR_NAME, toBytes("my-connector")),
                        String.format("Missing required header %s", ERROR_HEADER_STAGE)
                ),
                Arguments.of(new RecordHeaders()
                                .add(ERROR_HEADER_STAGE, toBytes("VALUE_CONVERTER"))
                                .add(ERROR_HEADER_TASK_ID, toBytes(2))
                                .add(ERROR_HEADER_CONNECTOR_NAME, toBytes("my-connector")),
                        String.format("Missing required header %s", ERROR_HEADER_EXECUTING_CLASS)
                ),
                Arguments.of(new RecordHeaders()
                                .add(ERROR_HEADER_STAGE, toBytes("VALUE_CONVERTER"))
                                .add(ERROR_HEADER_EXECUTING_CLASS, toBytes("org.apache.kafka.connect.json"
                                        + ".JsonConverter"))
                                .add(ERROR_HEADER_CONNECTOR_NAME, toBytes("my-connector")),
                        String.format("Missing required header %s", ERROR_HEADER_TASK_ID)
                ),
                Arguments.of(new RecordHeaders()
                                .add(ERROR_HEADER_STAGE, toBytes("VALUE_CONVERTER"))
                                .add(ERROR_HEADER_EXECUTING_CLASS, toBytes("org.apache.kafka.connect.json"
                                        + ".JsonConverter"))
                                .add(ERROR_HEADER_TASK_ID, toBytes(2)),
                        String.format("Missing required header %s", ERROR_HEADER_CONNECTOR_NAME)
                )
        );
    }

    static Stream<Arguments> generateNonNullableHeaders() {
        return Stream.of(
                Arguments.of(generateDefaultHeaders()
                        .add(ERROR_HEADER_ORIG_PARTITION, null), "Cannot parse int from null"
                ),
                Arguments.of(generateDefaultHeaders()
                        .add(ERROR_HEADER_ORIG_OFFSET, null), "Cannot parse long from null"
                ),
                Arguments.of(new RecordHeaders()
                        .add(ERROR_HEADER_STAGE, toBytes("VALUE_CONVERTER"))
                        .add(ERROR_HEADER_EXECUTING_CLASS, toBytes("org.apache.kafka.connect.json.JsonConverter"))
                        .add(ERROR_HEADER_TASK_ID, toBytes(2))
                        .add(ERROR_HEADER_TASK_ID, null), "Cannot parse int from null"
                )
        );
    }

    private static Headers generateDefaultHeaders() {
        return new RecordHeaders()
                .add(ERROR_HEADER_STAGE, toBytes("VALUE_CONVERTER"))
                .add(ERROR_HEADER_EXECUTING_CLASS, toBytes("org.apache.kafka.connect.json.JsonConverter"))
                .add(ERROR_HEADER_TASK_ID, toBytes(2))
                .add(ERROR_HEADER_CONNECTOR_NAME, toBytes("my-connector"));
    }

    @Test
    void shouldConvert() {
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
        this.softly.assertThat(new ConnectDeadLetterConverter().convert("foo", headers))
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
                    this.softly.assertThat(deadLetter.getDescription())
                            .isEqualTo("Error in stage VALUE_CONVERTER (org.apache.kafka.connect.json.JsonConverter) "
                                    + "in my-connector[2]");
                });
    }

    @Test
    void shouldConvertWithMissingHeaders() {
        final Headers headers = generateDefaultHeaders();
        this.softly.assertThat(new ConnectDeadLetterConverter().convert("foo", headers))
                .satisfies(deadLetter -> {
                    this.softly.assertThat(deadLetter.getPartition()).isNotPresent();
                    this.softly.assertThat(deadLetter.getTopic()).isNotPresent();
                    this.softly.assertThat(deadLetter.getOffset()).isNotPresent();
                    final ErrorDescription cause = deadLetter.getCause();
                    this.softly.assertThat(cause.getErrorClass()).isNotPresent();
                    this.softly.assertThat(cause.getMessage()).isNotPresent();
                    this.softly.assertThat(cause.getStackTrace()).isNotPresent();
                });
    }

    @Test
    void shouldConvertWithNullHeaders() {
        final Headers headers = generateDefaultHeaders()
                .add(ERROR_HEADER_EXCEPTION_MESSAGE, null);
        this.softly.assertThat(new ConnectDeadLetterConverter().convert("foo", headers))
                .satisfies(deadLetter -> this.softly.assertThat(deadLetter.getCause().getMessage()).isNotPresent());
    }

    @ParameterizedTest
    @MethodSource("generateMissingRequiredHeaders")
    void shouldThrowWithMissingRequiredHeaders(final Headers headers, final String message) {
        this.softly.assertThatThrownBy(() -> new ConnectDeadLetterConverter().convert("foo", headers))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
    }

    @ParameterizedTest
    @MethodSource("generateNonNullableHeaders")
    void shouldThrowWithNonNullableHeaders(final Headers headers, final String message) {
        this.softly.assertThatThrownBy(() -> new ConnectDeadLetterConverter().convert("foo", headers))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
    }

}