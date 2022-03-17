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
import org.apache.kafka.common.header.Headers;

@FunctionalInterface
interface DeadLetterConverter {

    /**
     * Get the value of the header as an {@code int}
     *
     * @param header header
     * @return {@code int} value of header
     */
    static int intValue(final Header header) {
        return stringValue(header)
                .map(Integer::parseInt)
                .orElseThrow(illegalArgument("Cannot parse int from null"));
    }

    /**
     * Get the value of the header as a {@code long}
     *
     * @param header header
     * @return {@code long} value of header
     */
    static long longValue(final Header header) {
        return stringValue(header)
                .map(Long::parseLong)
                .orElseThrow(illegalArgument("Cannot parse long from null"));
    }

    /**
     * Get the optional value of the header as a {@link String}
     *
     * @param header header
     * @return Optional {@link String} value of header
     */
    static Optional<String> stringValue(final Header header) {
        final byte[] value = header.value();
        return Optional.ofNullable(value)
                .map(String::new);
    }

    /**
     * Retrieve a header from headers
     *
     * @param headers headers to retrieve header from
     * @param key key of header to retrieve
     * @return optional header
     */
    static Optional<Header> getHeader(final Headers headers, final String key) {
        return Optional.ofNullable(headers.lastHeader(key));
    }

    /**
     * Create a supplier of an {@link IllegalArgumentException} stating that a required header is missing
     *
     * @param header Message template used as the message of the exception
     * @return Supplier of an {@link IllegalArgumentException}
     */
    static Supplier<IllegalArgumentException> missingRequiredHeader(final String header) {
        return illegalArgument("Missing required header %s", header);
    }

    /**
     * Create a supplier of an {@link IllegalArgumentException}
     *
     * @param message Message template used as the message of the exception
     * @param args Parameters passed to {@code message} using {@link String#format(String, Object...)}
     * @return Supplier of an {@link IllegalArgumentException} with formatted message
     */
    private static Supplier<IllegalArgumentException> illegalArgument(final String message, final Object... args) {
        return () -> new IllegalArgumentException(String.format(message, args));
    }

    /**
     * Create a {@link DeadLetter} from any value and associated headers.
     *
     * @param value value to create dead letter from
     * @param headers headers to retrieve meta information such as topic, partition, and offset from.
     * @return {@link DeadLetter} object representing error
     */
    DeadLetter convert(Object value, Headers headers);
}
