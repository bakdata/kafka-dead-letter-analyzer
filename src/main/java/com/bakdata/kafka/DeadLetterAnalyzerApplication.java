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

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes.StringSerde;

/**
 * A Kafka Streams application that analyzes dead letters in your Kafka cluster
 */
@Slf4j
public final class DeadLetterAnalyzerApplication implements LargeMessageStreamsApp {

    public static void main(final String[] args) {
        KafkaApplication.startApplication(new SimpleKafkaStreamsApplication(DeadLetterAnalyzerApplication::new), args);
    }

    @Override
    public void buildTopology(final TopologyBuilder topologyBuilder) {
        new DeadLetterAnalyzerTopology(topologyBuilder).buildTopology();
    }

    @Override
    public Map<String, Object> createKafkaProperties() {
        final Map<String, Object> kafkaProperties = new HashMap<>();
        kafkaProperties.put(LargeMessageSerdeConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        kafkaProperties.put(AbstractLargeMessageConfig.USE_HEADERS_CONFIG, true);
        return kafkaProperties;
    }

    @Override
    public SerdeConfig defaultSerializationConfig() {
        return new SerdeConfig(StringSerde.class, LargeMessageSerde.class);
    }

    @Override
    public String getUniqueAppId(final StreamsTopicConfig streamsTopicConfig) {
        return "dead-letter-analyzer-" + streamsTopicConfig.getOutputTopic();
    }

}
