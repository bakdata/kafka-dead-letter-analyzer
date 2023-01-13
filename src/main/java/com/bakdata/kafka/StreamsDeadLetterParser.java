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

import static com.bakdata.kafka.ErrorHeaderProcessor.DESCRIPTION;
import static com.bakdata.kafka.ErrorHeaderProcessor.EXCEPTION_CLASS_NAME;
import static com.bakdata.kafka.ErrorHeaderProcessor.EXCEPTION_MESSAGE;
import static com.bakdata.kafka.ErrorHeaderProcessor.EXCEPTION_STACK_TRACE;
import static com.bakdata.kafka.ErrorHeaderProcessor.OFFSET;
import static com.bakdata.kafka.ErrorHeaderProcessor.PARTITION;
import static com.bakdata.kafka.ErrorHeaderProcessor.TOPIC;
import static com.bakdata.kafka.HeaderHelper.getHeader;
import static com.bakdata.kafka.HeaderHelper.missingRequiredHeader;
import static com.bakdata.kafka.HeaderHelper.stringValue;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.header.Headers;

@RequiredArgsConstructor
class StreamsDeadLetterParser implements DeadLetterParser {

    static final String FAULTY_OFFSET_HEADER = "HEADER_PREFIX + offset";

    @Override
    public DeadLetter convert(final Object value, final Headers headers) {
        final int partition = getHeader(headers, PARTITION)
                .map(HeaderHelper::intValue)
                .orElseThrow(missingRequiredHeader(PARTITION));
        final String topic = getHeader(headers, TOPIC)
                .flatMap(HeaderHelper::stringValue)
                .orElseThrow(missingRequiredHeader(TOPIC));
        final long offset = getHeader(headers, OFFSET)
                // This was a faulty header that has been set in the error handling library.
                // We support it for backwards compatibility
                .or(() -> getHeader(headers, FAULTY_OFFSET_HEADER))
                .map(HeaderHelper::longValue)
                .orElseThrow(missingRequiredHeader(OFFSET));
        final String description = getHeader(headers, DESCRIPTION)
                .flatMap(HeaderHelper::stringValue)
                .orElseThrow(missingRequiredHeader(DESCRIPTION));
        final String errorClass = getHeader(headers, EXCEPTION_CLASS_NAME)
                .flatMap(HeaderHelper::stringValue)
                .orElseThrow(missingRequiredHeader(EXCEPTION_CLASS_NAME));
        final String message = stringValue(getHeader(headers, EXCEPTION_MESSAGE)
                .orElseThrow(missingRequiredHeader(EXCEPTION_MESSAGE)))
                .orElse(null);
        final String stackTrace = getHeader(headers, EXCEPTION_STACK_TRACE)
                .flatMap(HeaderHelper::stringValue)
                .orElseThrow(missingRequiredHeader(EXCEPTION_STACK_TRACE));
        final ErrorDescription errorDescription = ErrorDescription.newBuilder()
                .setErrorClass(errorClass)
                .setMessage(message)
                .setStackTrace(stackTrace)
                .build();
        return DeadLetter.newBuilder()
                .setPartition(partition)
                .setTopic(topic)
                .setOffset(offset)
                .setInputValue(Optional.ofNullable(value).map(ErrorUtil::toString).orElse(null))
                .setDescription(description)
                .setCause(errorDescription)
                .build();
    }
}
