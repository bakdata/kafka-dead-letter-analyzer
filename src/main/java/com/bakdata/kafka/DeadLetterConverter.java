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

import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.header.Header;

@FunctionalInterface
interface DeadLetterConverter {
    static int intValue(final Header header) {
        return stringValue(header)
                .map(Integer::parseInt)
                .orElseThrow(illegalArgument("Cannot parse int from null"));
    }

    static long longValue(final Header header) {
        return stringValue(header)
                .map(Long::parseLong)
                .orElseThrow(illegalArgument("Cannot parse long from null"));
    }

    static Supplier<IllegalArgumentException> illegalArgument(final String message, final Object... args) {
        return () -> new IllegalArgumentException(String.format(message, args));
    }

    static Optional<String> stringValue(final Header header) {
        final byte[] value = header.value();
        return Optional.ofNullable(value)
                .map(String::new);
    }

    DeadLetter convert(Object value);
}
