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

import static com.bakdata.kafka.DeadLetterConverter.illegalArgument;
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

import java.util.Optional;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

@RequiredArgsConstructor
class ConnectDeadLetterConverter implements DeadLetterConverter {
    private final @NonNull Headers headers;

    @Override
    public DeadLetter convert(final Object value) {
        final Optional<Integer> partition = this.getHeader(ERROR_HEADER_ORIG_PARTITION)
                .map(DeadLetterConverter::intValue);
        final Optional<String> topic = this.getHeader(ERROR_HEADER_ORIG_TOPIC)
                .flatMap(DeadLetterConverter::stringValue);
        final Optional<Long> offset = this.getHeader(ERROR_HEADER_ORIG_OFFSET)
                .map(DeadLetterConverter::longValue);
        final String stage = this.getHeader(ERROR_HEADER_STAGE)
                .flatMap(DeadLetterConverter::stringValue)
                .orElseThrow(illegalArgument("Missing required header %s", ERROR_HEADER_STAGE));
        final String clazz = this.getHeader(ERROR_HEADER_EXECUTING_CLASS)
                .flatMap(DeadLetterConverter::stringValue)
                .orElseThrow(illegalArgument("Missing required header %s", ERROR_HEADER_EXECUTING_CLASS));
        final Optional<String> errorClass = this.getHeader(ERROR_HEADER_EXCEPTION)
                .flatMap(DeadLetterConverter::stringValue);
        final int taskId = this.getHeader(ERROR_HEADER_TASK_ID)
                .map(DeadLetterConverter::intValue)
                .orElseThrow(illegalArgument("Missing required header %s", ERROR_HEADER_TASK_ID));
        final String connectorName = this.getHeader(ERROR_HEADER_CONNECTOR_NAME)
                .flatMap(DeadLetterConverter::stringValue)
                .orElseThrow(illegalArgument("Missing required header %s", ERROR_HEADER_CONNECTOR_NAME));
        final Optional<String> message = this.getHeader(ERROR_HEADER_EXCEPTION_MESSAGE)
                .flatMap(DeadLetterConverter::stringValue);
        final Optional<String> stackTrace = this.getHeader(ERROR_HEADER_EXCEPTION_STACK_TRACE)
                .flatMap(DeadLetterConverter::stringValue);
        return DeadLetter.newBuilder()
                .setPartition(partition.orElse(null))
                .setTopic(topic.orElse(null))
                .setOffset(offset.orElse(null))
                .setInputValue(Optional.ofNullable(value).map(ErrorUtil::toString).orElse(null))
                .setDescription(
                        String.format("Error in stage %s (%s) in %s[%d]", stage, clazz, connectorName, taskId))
                .setCause(ErrorDescription.newBuilder()
                        .setErrorClass(errorClass.orElse(null))
                        .setMessage(message.orElse(null))
                        .setStackTrace(stackTrace.orElse(null))
                        .build())
                .build();
    }

    private Optional<Header> getHeader(final String key) {
        return Optional.ofNullable(this.headers.lastHeader(key));
    }
}
