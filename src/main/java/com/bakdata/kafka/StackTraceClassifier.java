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

import com.google.common.base.Splitter;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
class StackTraceClassifier {
    private static final Pattern LINE =
            Pattern.compile("(?:\n|^)\tat ([._a-zA-Z0-9]+\\([_a-zA-Z0-9]+.java:\\d+\\))(?:\n|$)");
    private static final Pattern EXCEPTION = Pattern.compile("^([._a-zA-Z0-9$]+).*");

    static String classify(final String stackTrace) {
        return extractLine(stackTrace)
                .orElseGet(() -> extractExceptionFromFirstLine(stackTrace));
    }

    private static Optional<String> extractLine(final String stackTrace) {
        final Matcher matcher = LINE.matcher(stackTrace);
        if (matcher.find()) {
            return Optional.of(matcher.group(1));
        } else {
            log.warn("Cannot parse line from stack trace:\n'{}'", stackTrace);
            return Optional.empty();
        }
    }

    private static String extractExceptionFromFirstLine(final String stackTrace) {
        final String firstLine = Splitter.on("\n").splitToStream(stackTrace).findFirst().orElseThrow();
        final Matcher matcher = EXCEPTION.matcher(firstLine);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            log.warn("Cannot parse exception from stack trace:\n'{}'", stackTrace);
            return firstLine;
        }
    }
}
