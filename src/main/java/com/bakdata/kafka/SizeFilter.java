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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

@RequiredArgsConstructor
@Slf4j
class SizeFilter {
    private final int maxSize;

    private static String writeAsJson(final SpecificRecord specificRecord) throws IOException {
        final Schema schema = specificRecord.getSchema();
        final DatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(schema);
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, out);
            writer.write(specificRecord, jsonEncoder);
            jsonEncoder.flush();
            return out.toString();
        }
    }

    boolean filterMaxSize(final Object key, final SpecificRecord specificRecord) {
        try {
            final boolean isBelowMaxSize = writeAsJson(specificRecord).length() <= this.maxSize;
            if (!isBelowMaxSize) {
                log.warn("Filtering record {} because it exceeds the maximum record size", key);
            }
            return isBelowMaxSize;
        } catch (final IOException e) {
            throw new UncheckedIOException("Error determining size of record", e);
        }
    }
}
