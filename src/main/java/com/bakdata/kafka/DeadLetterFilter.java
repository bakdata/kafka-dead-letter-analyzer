package com.bakdata.kafka;

import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_NAME;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

class DeadLetterFilter implements FixedKeyProcessor<Object, Object, DeadLetter> {
    private FixedKeyProcessorContext<Object, DeadLetter> context;

    @Override
    public void init(final FixedKeyProcessorContext<Object, DeadLetter> context) {
        this.context = context;
    }

    @Override
    public void process(final FixedKeyRecord<Object, Object> record) {
        final Object object = record.value();
        if (object instanceof final DeadLetter deadLetter && !this.isNativeDeadLetter(record)) {
            this.context.forward(record.withValue(deadLetter));
        }
    }

    private boolean isNativeDeadLetter(final FixedKeyRecord<Object, Object> inputRecord) {
        final Headers headers = inputRecord.headers();
        // if we failed to analyze a DeadLetter in this app, a new dead letter is created with the native dlq headers
        // these dead letters need to be routed to NativeStreamsDeadLetterParser
        final Iterable<Header> allHeaders = headers.headers(HEADER_ERRORS_EXCEPTION_NAME);
        return allHeaders.iterator().hasNext();
    }

}
