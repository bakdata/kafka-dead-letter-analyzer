/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata
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

import static com.bakdata.kafka.DeadLetterAnalyzerTopology.REPARTITION_NAME;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Properties;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

/**
 * A Kafka Streams application that analyzes dead letters in your Kafka cluster
 */
@Slf4j
@ToString(callSuper = true)
@Setter
public final class DeadLetterAnalyzerApplication extends KafkaStreamsApplication {

    private static final String EXAMPLES_TOPIC_ROLE = "examples";
    private static final String STATS_TOPIC_ROLE = "stats";

    public static void main(final String[] args) {
        startApplication(new DeadLetterAnalyzerApplication(), args);
    }

    @Override
    public void buildTopology(final StreamsBuilder builder) {
        DeadLetterAnalyzerTopology.builder()
                .inputPattern(this.getInputPattern())
                .outputTopic(this.getOutputTopic())
                .statsTopic(this.getStatsTopic())
                .examplesTopic(this.getExamplesTopic())
                .errorTopic(this.getErrorTopic())
                .kafkaProperties(this.getKafkaProperties())
                .build()
                .buildTopology(builder);
    }

    @Override
    public Properties createKafkaProperties() {
        final Properties kafkaProperties = super.createKafkaProperties();
        kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        kafkaProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, LargeMessageSerde.class);
        kafkaProperties.put(LargeMessageSerdeConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        kafkaProperties.put(AbstractLargeMessageConfig.USE_HEADERS_CONFIG, true);
        return kafkaProperties;
    }

    @Override
    public String getUniqueAppId() {
        return "dead-letter-analyzer-" + this.getOutputTopic();
    }

    @Override
    protected void cleanUpRun(final CleanUpRunner cleanUpRunner) {
        super.cleanUpRun(cleanUpRunner);

        if (this.isDeleteOutputTopic()) {
            final Properties kafkaProperties = this.getKafkaProperties();
            final AbstractLargeMessageConfig largeMessageConfig = new AbstractLargeMessageConfig(kafkaProperties);
            final LargeMessageStoringClient storingClient = largeMessageConfig.getStorer();
            storingClient.deleteAllFiles(this.getOutputTopic());
            storingClient.deleteAllFiles(this.getErrorTopic());
            storingClient.deleteAllFiles(this.getExamplesTopic());
            storingClient.deleteAllFiles(this.getRepartitionTopic());
        }
    }

    private String getRepartitionTopic() {
        return this.getUniqueAppId() + REPARTITION_NAME + "-repartition";
    }

    private String getStatsTopic() {
        return this.getOutputTopic(STATS_TOPIC_ROLE);
    }

    private String getExamplesTopic() {
        return this.getOutputTopic(EXAMPLES_TOPIC_ROLE);
    }

}
