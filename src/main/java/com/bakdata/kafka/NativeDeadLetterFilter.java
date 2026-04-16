package com.bakdata.kafka;

import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_NAME;

import lombok.NoArgsConstructor;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

@NoArgsConstructor
class NativeDeadLetterFilter<K, V> implements FixedKeyProcessor<K, V, V> {
    private FixedKeyProcessorContext<K, V> context;

    private static <K, V> boolean isNativeDeadLetter(final FixedKeyRecord<K, V> inputRecord) {
        final Headers headers = inputRecord.headers();
        // if we failed to analyze a DeadLetter in this app, a new dead letter is created with the native dlq headers
        // these dead letters need to be routed to NativeStreamsDeadLetterParser
        final Iterable<Header> allHeaders = headers.headers(HEADER_ERRORS_EXCEPTION_NAME);
        return allHeaders.iterator().hasNext();
    }

    @Override
    public void init(final FixedKeyProcessorContext<K, V> context) {
        this.context = context;
    }

    @Override
    public void process(final FixedKeyRecord<K, V> inputRecord) {
        if (!isNativeDeadLetter(inputRecord)) {
            this.context.forward(inputRecord);
        }
    }

}
