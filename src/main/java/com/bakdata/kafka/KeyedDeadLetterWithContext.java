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

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Builder
@Value
class KeyedDeadLetterWithContext {

    @NonNull DeadLetterWithContext value;
    @NonNull ErrorKey key;

    FullDeadLetterWithContext format() {
        final Context context = this.value.getContext();
        return FullDeadLetterWithContext.newBuilder()
                .setDeadLetter(this.value.getDeadLetter())
                .setTopic(this.key.getTopic())
                .setType(this.key.getType())
                .setPartition(context.getPartition())
                .setKey(context.getKey())
                .setOffset(context.getOffset())
                .setTimestamp(Formatter.format(context.getTimestamp()))
                .build();
    }

    String extractElasticKey() {
        final Context context = this.value.getContext();
        return String.format("%s+%d+%d", this.key.getTopic(), context.getPartition(), context.getOffset());
    }
}
