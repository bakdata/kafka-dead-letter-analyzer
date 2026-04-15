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
import static com.bakdata.kafka.DescribingProcessingExceptionHandler.HEADER_ERRORS_PROCESSOR_NODE_ID_NAME;
import static com.bakdata.kafka.DescribingProcessingExceptionHandler.HEADER_ERRORS_TASK_ID_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_MESSAGE_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_OFFSET_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_PARTITION_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_STACKTRACE_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_TOPIC_NAME;

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
class NativeStreamsDeadLetterParserTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    static Stream<Arguments> generateMissingRequiredHeaders() {
        return Stream.of(
                Arguments.of(new RecordHeaders()
                                .add(HEADER_ERRORS_OFFSET_NAME, toBytes(10L))
                                .add(HEADER_ERRORS_EXCEPTION_NAME, toBytes("org.apache.kafka.connect.errors"
                                        + ".DataException"))
                                .add(HEADER_ERRORS_STACKTRACE_NAME, toBytes(StackTraceClassifierTest.STACK_TRACE)),
                        String.format("Missing required header %s", HEADER_ERRORS_PARTITION_NAME)
                ),
                Arguments.of(new RecordHeaders()
                                .add(HEADER_ERRORS_PARTITION_NAME, toBytes(1))
                                .add(HEADER_ERRORS_EXCEPTION_NAME, toBytes("org.apache.kafka.connect.errors"
                                        + ".DataException"))
                                .add(HEADER_ERRORS_STACKTRACE_NAME, toBytes(StackTraceClassifierTest.STACK_TRACE)),
                        String.format("Missing required header %s", HEADER_ERRORS_OFFSET_NAME)
                ),
                Arguments.of(new RecordHeaders()
                                .add(HEADER_ERRORS_PARTITION_NAME, toBytes(1))
                                .add(HEADER_ERRORS_OFFSET_NAME, toBytes(10L))
                                .add(HEADER_ERRORS_STACKTRACE_NAME, toBytes(StackTraceClassifierTest.STACK_TRACE)),
                        String.format("Missing required header %s", HEADER_ERRORS_EXCEPTION_NAME)
                ),
                Arguments.of(new RecordHeaders()
                                .add(HEADER_ERRORS_PARTITION_NAME, toBytes(1))
                                .add(HEADER_ERRORS_OFFSET_NAME, toBytes(10L))
                                .add(HEADER_ERRORS_EXCEPTION_NAME, toBytes("org.apache.kafka.connect.errors"
                                        + ".DataException")),
                        String.format("Missing required header %s", HEADER_ERRORS_STACKTRACE_NAME)
                )
        );
    }

    static Stream<Arguments> generateNonNullableHeaders() {
        return Stream.of(
                Arguments.of(generateDefaultHeaders()
                        .add(HEADER_ERRORS_PARTITION_NAME, null), "Cannot parse int from null"
                ),
                Arguments.of(generateDefaultHeaders()
                        .add(HEADER_ERRORS_OFFSET_NAME, null), "Cannot parse long from null"
                )
        );
    }

    private static Headers generateDefaultHeaders() {
        return new RecordHeaders()
                .add(HEADER_ERRORS_PARTITION_NAME, toBytes(1))
                .add(HEADER_ERRORS_TOPIC_NAME, toBytes("my-topic"))
                .add(HEADER_ERRORS_OFFSET_NAME, toBytes(10L))
                .add(HEADER_ERRORS_EXCEPTION_NAME, toBytes("org.apache.kafka.connect.errors.DataException"))
                .add(HEADER_ERRORS_EXCEPTION_MESSAGE_NAME, toBytes("my message"))
                .add(HEADER_ERRORS_STACKTRACE_NAME, toBytes(StackTraceClassifierTest.STACK_TRACE));
    }

    @Test
    void shouldConvert() {
        final Headers headers = new RecordHeaders()
                .add(HEADER_ERRORS_PARTITION_NAME, toBytes(1))
                .add(HEADER_ERRORS_TOPIC_NAME, toBytes("my-topic"))
                .add(HEADER_ERRORS_OFFSET_NAME, toBytes(10L))
                .add(HEADER_ERRORS_PROCESSOR_NODE_ID_NAME, toBytes("processor"))
                .add(HEADER_ERRORS_TASK_ID_NAME, toBytes("task"))
                .add(HEADER_ERRORS_EXCEPTION_NAME, toBytes("org.apache.kafka.connect.errors.DataException"))
                .add(HEADER_ERRORS_EXCEPTION_MESSAGE_NAME, toBytes("my message"))
                .add(HEADER_ERRORS_STACKTRACE_NAME, toBytes(StackTraceClassifierTest.STACK_TRACE));

        this.softly.assertThat(new NativeStreamsDeadLetterParser().convert("foo", headers, 200L))
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
                            .isEqualTo("Error in processor node processor in task task");
                    this.softly.assertThat(deadLetter.getInputTimestamp()).hasValue(Instant.ofEpochMilli(200L));
                });
    }

    @Test
    void shouldConvertNullTopicHeader() {
        final Headers headers = generateDefaultHeaders()
                .add(HEADER_ERRORS_TOPIC_NAME, null);
        this.softly.assertThat(new NativeStreamsDeadLetterParser().convert("foo", headers, 0))
                .satisfies(deadLetter -> this.softly.assertThat(deadLetter.getTopic()).isNotPresent());
    }

    @Test
    void shouldConvertNullMessageHeader() {
        final Headers headers = generateDefaultHeaders()
                .add(HEADER_ERRORS_EXCEPTION_MESSAGE_NAME, null);
        this.softly.assertThat(new NativeStreamsDeadLetterParser().convert("foo", headers, 0))
                .satisfies(deadLetter -> this.softly.assertThat(deadLetter.getCause().getMessage()).isNotPresent());
    }

    @Test
    void shouldConvertNullProcessorNodeIdAndTaskIdHeader() {
        final Headers headers = generateDefaultHeaders()
                .add(HEADER_ERRORS_PROCESSOR_NODE_ID_NAME, null)
                .add(HEADER_ERRORS_TASK_ID_NAME, null);
        this.softly.assertThat(new NativeStreamsDeadLetterParser().convert("foo", headers, 0))
                .satisfies(deadLetter -> this.softly.assertThat(deadLetter.getDescription())
                        .isEqualTo("Error in processor node [unknown] in task [unknown]"));
    }

    @ParameterizedTest
    @MethodSource({
            "generateMissingRequiredHeaders",
            "generateNonNullableHeaders",
    })
    void shouldThrowForInvalidHeaders(final Headers headers, final String message) {
        final DeadLetterParser parser = new NativeStreamsDeadLetterParser();
        this.softly.assertThatThrownBy(() -> parser.convert("foo", headers, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
    }

}
