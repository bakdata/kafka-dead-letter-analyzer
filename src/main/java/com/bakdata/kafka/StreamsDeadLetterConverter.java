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
import static com.bakdata.kafka.ErrorHeaderTransformer.DESCRIPTION;
import static com.bakdata.kafka.ErrorHeaderTransformer.EXCEPTION_CLASS_NAME;
import static com.bakdata.kafka.ErrorHeaderTransformer.EXCEPTION_MESSAGE;
import static com.bakdata.kafka.ErrorHeaderTransformer.EXCEPTION_STACK_TRACE;
import static com.bakdata.kafka.ErrorHeaderTransformer.OFFSET;
import static com.bakdata.kafka.ErrorHeaderTransformer.PARTITION;
import static com.bakdata.kafka.ErrorHeaderTransformer.TOPIC;

import java.util.Optional;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

@RequiredArgsConstructor
class StreamsDeadLetterConverter implements DeadLetterConverter {
    private final @NonNull Headers headers;

    @Override
    public DeadLetter convert(final Object value) {
        final Integer partition = this.getHeader(PARTITION)
                .map(DeadLetterConverter::intValue)
                .orElseThrow(illegalArgument(MISSING_REQUIRED_HEADER, PARTITION));
        final String topic = this.getHeader(TOPIC)
                .flatMap(DeadLetterConverter::stringValue)
                .orElseThrow(illegalArgument(MISSING_REQUIRED_HEADER, TOPIC));
        final Long offset = this.getHeader(OFFSET)
                .map(DeadLetterConverter::longValue)
                .orElseThrow(illegalArgument(MISSING_REQUIRED_HEADER, OFFSET));
        final String description = this.getHeader(DESCRIPTION)
                .flatMap(DeadLetterConverter::stringValue)
                .orElseThrow(illegalArgument(MISSING_REQUIRED_HEADER, DESCRIPTION));
        final String errorClass = this.getHeader(EXCEPTION_CLASS_NAME)
                .flatMap(DeadLetterConverter::stringValue)
                .orElseThrow(illegalArgument(MISSING_REQUIRED_HEADER, EXCEPTION_CLASS_NAME));
        final String message = this.getHeader(EXCEPTION_MESSAGE)
                .flatMap(DeadLetterConverter::stringValue)
                .orElseThrow(illegalArgument(MISSING_REQUIRED_HEADER, EXCEPTION_MESSAGE));
        final String stackTrace = this.getHeader(EXCEPTION_STACK_TRACE)
                .flatMap(DeadLetterConverter::stringValue)
                .orElseThrow(illegalArgument(MISSING_REQUIRED_HEADER, EXCEPTION_STACK_TRACE));
        return DeadLetter.newBuilder()
                .setPartition(partition)
                .setTopic(topic)
                .setOffset(offset)
                .setInputValue(Optional.ofNullable(value).map(ErrorUtil::toString).orElse(null))
                .setDescription(description)
                .setCause(ErrorDescription.newBuilder()
                        .setErrorClass(errorClass)
                        .setMessage(message)
                        .setStackTrace(stackTrace)
                        .build())
                .build();
    }

    private Optional<Header> getHeader(final String key) {
        return Optional.ofNullable(this.headers.lastHeader(key));
    }
}
