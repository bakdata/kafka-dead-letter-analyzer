/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

import static com.bakdata.kafka.ErrorHeaderProcessor.EXCEPTION_CLASS_NAME;
import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.ERROR_HEADER_CONNECTOR_NAME;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.List;
import java.util.Set;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

@RequiredArgsConstructor
class DeadLetterAnalyzerTopology {

    static final String EXAMPLES_TOPIC_LABEL = "examples";
    static final String STATS_TOPIC_LABEL = "stats";
    private static final String REPARTITION_NAME = "analyzed";
    private static final String STATISTICS_STORE_NAME = "statistics";
    private final @NonNull TopologyBuilder builder;

    static <T extends SpecificRecord> Preconfigured<Serde<T>> getSpecificAvroSerde() {
        final Serde<T> serde = new SpecificAvroSerde<>();
        return Preconfigured.create(serde);
    }

    static String getExamplesTopic(final StreamsTopicConfig topics) {
        return topics.getOutputTopic(EXAMPLES_TOPIC_LABEL);
    }

    static String getStatsTopic(final StreamsTopicConfig topics) {
        return topics.getOutputTopic(STATS_TOPIC_LABEL);
    }

    private static String toElasticKey(final ErrorKey key) {
        return String.format("%s:%s", key.getTopic(), key.getType());
    }

    private static ErrorExample toErrorExample(final ErrorKey errorKey,
            final DeadLetterWithContext deadLetterWithContext) {
        return ErrorExample.newBuilder()
                .setExample(format(deadLetterWithContext))
                .setType(errorKey.getType())
                .setTopic(errorKey.getTopic())
                .build();
    }

    private static ExampleDeadLetterWithContext format(final DeadLetterWithContext deadLetterWithContext) {
        final Context context = deadLetterWithContext.getContext();
        return ExampleDeadLetterWithContext.newBuilder()
                .setDeadLetter(deadLetterWithContext.getDeadLetter())
                .setKey(context.getKey())
                .setOffset(context.getOffset())
                .setTimestamp(Formatter.format(context.getTimestamp()))
                .setPartition(context.getPartition())
                .build();
    }

    private static List<DeadLetter> getDeadLetters(final Object object) {
        return object instanceof DeadLetter ? List.of((DeadLetter) object) : List.of();
    }

    private static Preconfigured<Serde<Object>> getInputSerde() {
        final Serde<Object> serde = new BruteForceSerde();
        return Preconfigured.create(serde);
    }

    private static <K> void toDeadLetterTopic(final ImprovedKStream<K, DeadLetter> connectDeadLetters) {
        connectDeadLetters
                .selectKey((k, v) -> ErrorUtil.toString(k))
                .toErrorTopic();
    }

    private <T> T configureForKeys(final Preconfigured<T> preconfigured) {
        return this.builder.createConfigurator().configureForKeys(preconfigured);
    }

    private <T> T configureForValues(final Preconfigured<T> preconfigured) {
        return this.builder.createConfigurator().configureForValues(preconfigured);
    }

    private static <K> ImprovedKStream<K, KeyedDeadLetterWithContext> enrichWithContext(
            final ImprovedKStream<K, ? extends DeadLetter> allDeadLetters) {
        final ImprovedKStream<K, ProcessedValue<DeadLetter, KeyedDeadLetterWithContext>> processedDeadLetters =
                allDeadLetters.processValues(
                        ErrorCapturingValueProcessor.captureErrors(ContextEnricher::new));

        final ImprovedKStream<K, DeadLetter> analysisDeadLetters =
                processedDeadLetters.flatMapValues(ProcessedValue::getErrors)
                        .processValues(AvroDeadLetterConverter.asProcessor("Error analyzing dead letter"));
        toDeadLetterTopic(analysisDeadLetters);

        return processedDeadLetters.flatMapValues(ProcessedValue::getValues);
    }

    private static <K> ImprovedKStream<K, DeadLetter> streamHeaderDeadLetters(final ImprovedKStream<K, Object> input,
            final DeadLetterParser converterFactory) {
        final ImprovedKStream<K, ProcessedValue<Object, DeadLetter>> processedInput = input.processValues(
                ErrorCapturingValueProcessor.captureErrors(
                        () -> new DeadLetterParserTransformer<>(converterFactory)));
        final ImprovedKStream<K, DeadLetter> deadLetters =
                processedInput.flatMapValues(ProcessedValue::getErrors)
                        .processValues(
                                AvroDeadLetterConverter.asProcessor("Error converting errors to dead letters"));
        toDeadLetterTopic(deadLetters);

        return processedInput.flatMapValues(ProcessedValue::getValues);
    }

    void buildTopology() {
        final ImprovedKStream<Object, DeadLetter> allDeadLetters = this.streamDeadLetters();
        final ImprovedKStream<Object, KeyedDeadLetterWithContext> deadLettersWithContext =
                enrichWithContext(allDeadLetters);
        deadLettersWithContext
                .selectKey((k, v) -> v.extractElasticKey())
                .mapValues(KeyedDeadLetterWithContext::format)
                .toOutputTopic();

        final ImprovedKStream<ErrorKey, Result> aggregated = this.aggregate(deadLettersWithContext);
        aggregated
                .mapValues((errorKey, result) -> result.toFullErrorStatistics(errorKey))
                .selectKey((k, v) -> toElasticKey(k))
                .toOutputTopic(STATS_TOPIC_LABEL, ConfiguredProduced.valueSerde(getSpecificAvroSerde()));
        aggregated
                .flatMapValues(Result::getExamples)
                .mapValues(DeadLetterAnalyzerTopology::toErrorExample)
                .selectKey((k, v) -> toElasticKey(k))
                .toOutputTopic(EXAMPLES_TOPIC_LABEL);
    }

    private StoreBuilder<KeyValueStore<ErrorKey, ErrorStatistics>> createStatisticsStore(
            final Serde<ErrorKey> errorKeySerde) {
        final KeyValueBytesStoreSupplier statisticsStoreSupplier = Stores.inMemoryKeyValueStore(STATISTICS_STORE_NAME);
        return Stores.keyValueStoreBuilder(statisticsStoreSupplier, errorKeySerde,
                this.configureForValues(getSpecificAvroSerde()));
    }

    private ImprovedKStream<Object, DeadLetter> streamDeadLetters() {
        final ImprovedKStream<Object, Object> rawDeadLetters = this.builder.streamInputPattern(
                ConfiguredConsumed.with(getInputSerde(), getInputSerde()));

        final ImprovedKStream<Object, DeadLetter> streamDeadLetters = rawDeadLetters
                .flatMapValues(DeadLetterAnalyzerTopology::getDeadLetters);

        final ImprovedKStream<Object, Object> rawStreamHeaderDeadLetters = rawDeadLetters
                .processValues(() -> new HeaderFilter<>(EXCEPTION_CLASS_NAME));
        final ImprovedKStream<Object, DeadLetter> streamHeaderDeadLetters =
                streamHeaderDeadLetters(rawStreamHeaderDeadLetters, new StreamsDeadLetterParser());

        final ImprovedKStream<Object, Object> rawConnectDeadLetters = rawDeadLetters
                .processValues(() -> new HeaderFilter<>(ERROR_HEADER_CONNECTOR_NAME));
        final ImprovedKStream<Object, DeadLetter> connectDeadLetters =
                streamHeaderDeadLetters(rawConnectDeadLetters, new ConnectDeadLetterParser());

        return streamDeadLetters.merge(connectDeadLetters)
                .merge(streamHeaderDeadLetters);
    }

    private ImprovedKStream<ErrorKey, Result> aggregate(
            final ImprovedKStream<?, KeyedDeadLetterWithContext> withContext) {
        final Serde<ErrorKey> errorKeySerde = this.configureForKeys(getSpecificAvroSerde());
        final StoreBuilder<KeyValueStore<ErrorKey, ErrorStatistics>> statisticsStore =
                this.createStatisticsStore(errorKeySerde);

        final ImprovedKStream<ErrorKey, DeadLetterWithContext> analyzed = withContext.selectKey((k, v) -> v.getKey())
                .mapValues(KeyedDeadLetterWithContext::getValue);
        final KErrorStream<ErrorKey, DeadLetterWithContext, ErrorKey, Result> processedAggregations = analyzed
                .repartition(
                        Repartitioned.<ErrorKey, DeadLetterWithContext>as(REPARTITION_NAME).withKeySerde(errorKeySerde))
                .processValuesCapturingErrors(
                        new FixedKeyProcessorSupplier<>() {
                            @Override
                            public FixedKeyProcessor<ErrorKey, DeadLetterWithContext, Result> get() {
                                return new ErrorAggregatingProcessor(statisticsStore.name());
                            }

                            @Override
                            public Set<StoreBuilder<?>> stores() {
                                return Set.of(statisticsStore);
                            }
                        }
                );

        final ImprovedKStream<ErrorKey, DeadLetter> aggregationDeadLetters =
                processedAggregations.errors()
                        .processValues(AvroDeadLetterConverter.asProcessor("Error aggregating dead letters"));
        toDeadLetterTopic(aggregationDeadLetters);

        return processedAggregations.values();
    }

}
