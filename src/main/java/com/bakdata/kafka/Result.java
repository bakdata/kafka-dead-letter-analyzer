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
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.jooq.lambda.Seq;

@Builder
class Result {
    @Getter
    private final @NonNull ErrorStatistics statistics;
    private final DeadLetterWithContext example;

    Iterable<DeadLetterWithContext> getExamples() {
        return Seq.seq(Optional.ofNullable(this.example));
    }

    FullErrorStatistics toFullErrorStatistics(final ErrorKey errorKey) {
        return FullErrorStatistics.newBuilder()
                .setCount(this.statistics.getCount())
                .setCreated(Formatter.format(this.statistics.getCreated()))
                .setUpdated(Formatter.format(this.statistics.getUpdated()))
                .setType(errorKey.getType())
                .setTopic(errorKey.getTopic())
                .build();
    }
}
