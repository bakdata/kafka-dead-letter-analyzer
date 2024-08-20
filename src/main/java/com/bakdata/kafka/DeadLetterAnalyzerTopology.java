/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

@RequiredArgsConstructor
@Getter
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

    void buildTopology() {
        final KStream<Object, DeadLetter> allDeadLetters = this.streamDeadLetters();
        final KStream<Object, KeyedDeadLetterWithContext> deadLettersWithContext =
                this.enrichWithContext(allDeadLetters);
        deadLettersWithContext
                .selectKey((k, v) -> v.extractElasticKey())
                .mapValues(KeyedDeadLetterWithContext::format)
                .to(this.getOutputTopic());

        final KStream<ErrorKey, Result> aggregated = this.aggregate(deadLettersWithContext);
        aggregated
                .mapValues((errorKey, result) -> result.toFullErrorStatistics(errorKey))
                .selectKey((k, v) -> toElasticKey(k))
                .to(this.getStatsTopic(),
                        Produced.valueSerde(
                                this.configureForValues(getSpecificAvroSerde())));
        aggregated
                .flatMapValues(Result::getExamples)
                .mapValues(DeadLetterAnalyzerTopology::toErrorExample)
                .selectKey((k, v) -> toElasticKey(k))
                .to(this.getExamplesTopic());
    }

    String getExamplesTopic() {
        return this.builder.getTopics().getOutputTopic(EXAMPLES_TOPIC_LABEL);
    }

    String getStatsTopic() {
        return this.builder.getTopics().getOutputTopic(STATS_TOPIC_LABEL);
    }

    String getOutputTopic() {
        return this.builder.getTopics().getOutputTopic();
    }

    String getErrorTopic() {
        return this.builder.getTopics().getErrorTopic();
    }

    <T> T configureForKeys(final Preconfigured<T> preconfigured) {
        return this.builder.createConfigurator().configureForKeys(preconfigured);
    }

    <T> T configureForValues(final Preconfigured<T> preconfigured) {
        return this.builder.createConfigurator().configureForValues(preconfigured);
    }

    private KStream<Object, DeadLetter> streamDeadLetters() {
        final KStream<Object, Object> rawDeadLetters = this.builder.streamInputPattern(
                Consumed.with(this.configureForKeys(getInputSerde()),
                        this.configureForValues(getInputSerde())));

        final KStream<Object, DeadLetter> streamDeadLetters = rawDeadLetters
                .flatMapValues(DeadLetterAnalyzerTopology::getDeadLetters);

        final KStream<Object, Object> rawStreamHeaderDeadLetters = rawDeadLetters
                .processValues(() -> new HeaderFilter<>(EXCEPTION_CLASS_NAME));
        final KStream<Object, DeadLetter> streamHeaderDeadLetters =
                this.streamHeaderDeadLetters(rawStreamHeaderDeadLetters, new StreamsDeadLetterParser());

        final KStream<Object, Object> rawConnectDeadLetters = rawDeadLetters
                .processValues(() -> new HeaderFilter<>(ERROR_HEADER_CONNECTOR_NAME));
        final KStream<Object, DeadLetter> connectDeadLetters =
                this.streamHeaderDeadLetters(rawConnectDeadLetters, new ConnectDeadLetterParser());

        return streamDeadLetters.merge(connectDeadLetters)
                .merge(streamHeaderDeadLetters);
    }

    private <K> void toDeadLetterTopic(final KStream<K, DeadLetter> connectDeadLetters) {
        connectDeadLetters
                .selectKey((k, v) -> ErrorUtil.toString(k))
                .to(this.getErrorTopic());
    }

    private KStream<ErrorKey, Result> aggregate(final KStream<?, KeyedDeadLetterWithContext> withContext) {
        final Serde<ErrorKey> errorKeySerde =
                this.builder.createConfigurator().configureForKeys(getSpecificAvroSerde());
        final StoreBuilder<KeyValueStore<ErrorKey, ErrorStatistics>> statisticsStore =
                this.createStatisticsStore(errorKeySerde);

        final KStream<ErrorKey, DeadLetterWithContext> analyzed = withContext.selectKey((k, v) -> v.getKey())
                .mapValues(KeyedDeadLetterWithContext::getValue);
        final KStream<ErrorKey, ProcessedValue<DeadLetterWithContext, Result>> processedAggregations = analyzed
                .repartition(
                        Repartitioned.<ErrorKey, DeadLetterWithContext>as(REPARTITION_NAME).withKeySerde(errorKeySerde))
                .processValues(ErrorCapturingValueProcessor.captureErrors(
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
                ));

        final KStream<ErrorKey, DeadLetter> aggregationDeadLetters =
                processedAggregations.flatMapValues(ProcessedValue::getErrors)
                        .processValues(AvroDeadLetterConverter.asProcessor("Error aggregating dead letters"));
        this.toDeadLetterTopic(aggregationDeadLetters);

        return processedAggregations.flatMapValues(ProcessedValue::getValues);
    }

    private StoreBuilder<KeyValueStore<ErrorKey, ErrorStatistics>> createStatisticsStore(
            final Serde<ErrorKey> errorKeySerde) {
        final KeyValueBytesStoreSupplier statisticsStoreSupplier = Stores.inMemoryKeyValueStore(STATISTICS_STORE_NAME);
        return Stores.keyValueStoreBuilder(statisticsStoreSupplier, errorKeySerde,
                this.configureForValues(getSpecificAvroSerde()));
    }

    private <K> KStream<K, KeyedDeadLetterWithContext> enrichWithContext(
            final KStream<K, ? extends DeadLetter> allDeadLetters) {
        final KStream<K, ProcessedValue<DeadLetter, KeyedDeadLetterWithContext>> processedDeadLetters =
                allDeadLetters.processValues(
                        ErrorCapturingValueProcessor.captureErrors(ContextEnricher::new));

        final KStream<K, DeadLetter> analysisDeadLetters =
                processedDeadLetters.flatMapValues(ProcessedValue::getErrors)
                        .processValues(AvroDeadLetterConverter.asProcessor("Error analyzing dead letter"));
        this.toDeadLetterTopic(analysisDeadLetters);

        return processedDeadLetters.flatMapValues(ProcessedValue::getValues);
    }

    private <K> KStream<K, DeadLetter> streamHeaderDeadLetters(final KStream<K, Object> input,
            final DeadLetterParser converterFactory) {
        final KStream<K, ProcessedValue<Object, DeadLetter>> processedInput = input.processValues(
                ErrorCapturingValueProcessor.captureErrors(
                        () -> new DeadLetterParserTransformer<>(converterFactory)));
        final KStream<K, DeadLetter> deadLetters =
                processedInput.flatMapValues(ProcessedValue::getErrors)
                        .processValues(
                                AvroDeadLetterConverter.asProcessor("Error converting errors to dead letters"));
        this.toDeadLetterTopic(deadLetters);

        return processedInput.flatMapValues(ProcessedValue::getValues);
    }

}
