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

import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

@RequiredArgsConstructor
class ContextEnricher implements ValueTransformerWithKey<Object, DeadLetter, KeyedDeadLetterWithContext> {
    private ProcessorContext context;

    private static String extractType(final DeadLetter value) {
        final String stackTrace = value.getCause().getStackTrace().orElseThrow();
        return StackTraceClassifier.classify(stackTrace);
    }

    @Override
    public void init(final ProcessorContext context) {
        this.context = context;
    }

    @Override
    public KeyedDeadLetterWithContext transform(final Object key, final DeadLetter deadLetter) {
        final Context context = Context.newBuilder()
                .setKey(ErrorUtil.toString(key))
                .setOffset(this.context.offset())
                .setPartition(this.context.partition())
                .setTimestamp(Instant.ofEpochMilli(this.context.timestamp()))
                .build();
        final DeadLetterWithContext deadLetterWithContext = DeadLetterWithContext.newBuilder()
                .setContext(context)
                .setDeadLetter(deadLetter)
                .build();
        final ErrorKey errorKey = ErrorKey.newBuilder()
                .setTopic(this.context.topic())
                .setType(extractType(deadLetter))
                .build();
        return KeyedDeadLetterWithContext.builder()
                .value(deadLetterWithContext)
                .key(errorKey)
                .build();
    }

    @Override
    public void close() {
        //do nothing
    }
}
