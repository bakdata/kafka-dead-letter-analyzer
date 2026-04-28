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

import static com.bakdata.kafka.FilteringProcessingExceptionHandler.HEADER_ERRORS_PROCESSOR_NODE_ID_NAME;
import static com.bakdata.kafka.FilteringProcessingExceptionHandler.HEADER_ERRORS_TASK_ID_NAME;
import static com.bakdata.kafka.HeaderHelper.getHeader;
import static com.bakdata.kafka.HeaderHelper.missingRequiredHeader;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_MESSAGE_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_OFFSET_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_PARTITION_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_STACKTRACE_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_TOPIC_NAME;

import java.time.Instant;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.header.Headers;

@RequiredArgsConstructor
class NativeStreamsDeadLetterParser implements DeadLetterParser {

    @Override
    public DeadLetter convert(final Object value, final Headers headers, final long recordTimestamp) {
        final int partition = getHeader(headers, HEADER_ERRORS_PARTITION_NAME)
                .map(HeaderHelper::intValue)
                .orElseThrow(missingRequiredHeader(HEADER_ERRORS_PARTITION_NAME));
        final Optional<String> topic = getHeader(headers, HEADER_ERRORS_TOPIC_NAME)
                .flatMap(HeaderHelper::stringValue);
        final long offset = getHeader(headers, HEADER_ERRORS_OFFSET_NAME)
                .map(HeaderHelper::longValue)
                .orElseThrow(missingRequiredHeader(HEADER_ERRORS_OFFSET_NAME));
        final Optional<String> processorNodeId = getHeader(headers, HEADER_ERRORS_PROCESSOR_NODE_ID_NAME)
                .flatMap(HeaderHelper::stringValue);
        final Optional<String> taskId = getHeader(headers, HEADER_ERRORS_TASK_ID_NAME)
                .flatMap(HeaderHelper::stringValue);
        final String errorClass = getHeader(headers, HEADER_ERRORS_EXCEPTION_NAME)
                .flatMap(HeaderHelper::stringValue)
                .orElseThrow(missingRequiredHeader(HEADER_ERRORS_EXCEPTION_NAME));
        final Optional<String> message = getHeader(headers, HEADER_ERRORS_EXCEPTION_MESSAGE_NAME)
                .flatMap(HeaderHelper::stringValue);
        final String stackTrace = getHeader(headers, HEADER_ERRORS_STACKTRACE_NAME)
                .flatMap(HeaderHelper::stringValue)
                .orElseThrow(missingRequiredHeader(HEADER_ERRORS_STACKTRACE_NAME));
        final ErrorDescription errorDescription = ErrorDescription.newBuilder()
                .setErrorClass(errorClass)
                .setMessage(message.orElse(null))
                .setStackTrace(stackTrace)
                .build();
        final String description =
                String.format("Error in processor node %s in task %s", processorNodeId.orElse("[unknown]"),
                        taskId.orElse("[unknown]"));
        return DeadLetter.newBuilder()
                .setPartition(partition)
                .setTopic(topic.orElse(null))
                .setOffset(offset)
                .setInputValue(Optional.ofNullable(value).map(ErrorUtil::toString).orElse(null))
                .setDescription(description)
                .setCause(errorDescription)
                // Kafka Streams propagates the timestamp of the original message
                .setInputTimestamp(Instant.ofEpochMilli(recordTimestamp))
                .build();
    }
}
