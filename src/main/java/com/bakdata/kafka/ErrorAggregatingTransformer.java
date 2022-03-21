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
import java.util.Optional;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jooq.lambda.Seq;

@RequiredArgsConstructor
class ErrorAggregatingTransformer
        implements ValueTransformerWithKey<ErrorKey, DeadLetterWithContext, Result> {
    private final @NonNull String storeName;
    private KeyValueStore<ErrorKey, ErrorStatistics> statisticsStore;

    private static Result updatedResult(final ErrorStatistics oldStatistics, final ErrorStatistics newStatistics) {
        final ErrorStatistics updatedStatistics = merge(oldStatistics, newStatistics);
        return Result.builder()
                .statistics(updatedStatistics)
                .build();
    }

    private static ErrorStatistics merge(final ErrorStatistics oldStatistics, final ErrorStatistics newStatistics) {
        return ErrorStatistics.newBuilder()
                .setCount(oldStatistics.getCount() + newStatistics.getCount())
                .setCreated(Seq.of(oldStatistics.getCreated(), newStatistics.getCreated()).min().orElseThrow())
                .setUpdated(Seq.of(oldStatistics.getUpdated(), newStatistics.getUpdated()).max().orElseThrow())
                .build();
    }

    private static Result newResult(final DeadLetterWithContext deadLetterWithContext,
            final ErrorStatistics newStatistics) {
        return Result.builder()
                .example(deadLetterWithContext)
                .statistics(newStatistics)
                .build();
    }

    private static ErrorStatistics newStatistics(final DeadLetterWithContext deadLetterWithContext) {
        final Instant timestamp = deadLetterWithContext.getContext().getTimestamp();
        return ErrorStatistics.newBuilder()
                .setCount(1)
                .setCreated(timestamp)
                .setUpdated(timestamp)
                .build();
    }

    @Override
    public void init(final ProcessorContext context) {
        this.statisticsStore = context.getStateStore(this.storeName);
    }

    @Override
    public Result transform(final ErrorKey key, final DeadLetterWithContext deadLetterWithContext) {
        final ErrorStatistics newStatistics = newStatistics(deadLetterWithContext);
        final Result result = this.getStatistics(key)
                .map(oldStatistics -> updatedResult(oldStatistics, newStatistics))
                .orElseGet(() -> newResult(deadLetterWithContext, newStatistics));
        this.statisticsStore.put(key, result.getStatistics());
        return result;
    }

    @Override
    public void close() {
        //do nothing
    }

    private Optional<ErrorStatistics> getStatistics(final ErrorKey key) {
        return Optional.ofNullable(this.statisticsStore.get(key));
    }
}
