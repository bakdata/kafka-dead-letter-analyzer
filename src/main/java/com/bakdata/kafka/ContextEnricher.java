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

import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;

@RequiredArgsConstructor
class ContextEnricher implements FixedKeyProcessor<Object, DeadLetter, KeyedDeadLetterWithContext> {
    private FixedKeyProcessorContext<Object, KeyedDeadLetterWithContext> context;

    private static String extractType(final DeadLetter value) {
        final String stackTrace = value.getCause().getStackTrace().orElseThrow();
        return StackTraceClassifier.classify(stackTrace);
    }

    @Override
    public void init(final FixedKeyProcessorContext<Object, KeyedDeadLetterWithContext> context) {
        this.context = context;
    }

    @Override
    public void process(final FixedKeyRecord<Object, DeadLetter> inputRecord) {
        final Object key = inputRecord.key();
        final DeadLetter deadLetter = inputRecord.value();
        final RecordMetadata recordMetadata =
                this.context.recordMetadata().orElseThrow(() -> new IllegalArgumentException("No metadata available"));
        final Context deadLetterContext = Context.newBuilder()
                .setKey(ErrorUtil.toString(key))
                .setOffset(recordMetadata.offset())
                .setPartition(recordMetadata.partition())
                .setTimestamp(Instant.ofEpochMilli(inputRecord.timestamp()))
                .build();
        final DeadLetterWithContext deadLetterWithContext = DeadLetterWithContext.newBuilder()
                .setContext(deadLetterContext)
                .setDeadLetter(deadLetter)
                .build();
        final ErrorKey errorKey = ErrorKey.newBuilder()
                .setTopic(recordMetadata.topic())
                .setType(extractType(deadLetter))
                .build();
        final KeyedDeadLetterWithContext keyedDeadLetterWithContext = KeyedDeadLetterWithContext.builder()
                .value(deadLetterWithContext)
                .key(errorKey)
                .build();
        this.context.forward(inputRecord.withValue(keyedDeadLetterWithContext));
    }

    @Override
    public void close() {
        //do nothing
    }
}
