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

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

@RequiredArgsConstructor
class HeaderFilter<K, V> implements FixedKeyProcessor<K, V, V> {
    private final String requiredHeader;
    private FixedKeyProcessorContext<K, V> context;

    @Override
    public void init(final FixedKeyProcessorContext<K, V> context) {
        this.context = context;
    }

    @Override
    public void process(final FixedKeyRecord<K, V> inputRecord) {
        if (this.hasHeader(inputRecord)) {
            this.context.forward(inputRecord);
        }
    }

    @Override
    public void close() {
        // do nothing
    }

    private boolean hasHeader(final FixedKeyRecord<K, V> inputRecord) {
        final Headers headers = inputRecord.headers();
        return this.hasHeader(headers);
    }

    private boolean hasHeader(final Headers allHeaders) {
        final Iterable<Header> headers = allHeaders.headers(this.requiredHeader);
        return headers.iterator().hasNext();
    }
}
