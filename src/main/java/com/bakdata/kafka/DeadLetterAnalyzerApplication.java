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

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Properties;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import picocli.CommandLine.Option;

@Slf4j
@ToString(callSuper = true)
@Setter
public final class DeadLetterAnalyzerApplication extends KafkaStreamsApplication {

    private static final String EXAMPLES = "examples";
    private static final String STATS = "stats";
    @Option(names = "--max-size", description = "Maximum size of dead letters to send to output and examples topic")
    private int maxSize = Integer.MAX_VALUE;

    public static void main(final String[] args) {
        KafkaStreamsApplication.startApplication(new DeadLetterAnalyzerApplication(), args);
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
                .sizeFilter(new SizeFilter(this.maxSize))
                .build()
                .build(builder);
    }

    @Override
    public Properties createKafkaProperties() {
        final Properties kafkaProperties = super.createKafkaProperties();
        kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        kafkaProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, LargeMessageSerde.class);
        kafkaProperties.put(LargeMessageSerdeConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
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
            final AbstractLargeMessageConfig largeMessageConfig =
                    new AbstractLargeMessageConfig(this.getKafkaProperties());
            largeMessageConfig.getStorer().deleteAllFiles(this.getOutputTopic());
            largeMessageConfig.getStorer().deleteAllFiles(this.getErrorTopic());
            largeMessageConfig.getStorer().deleteAllFiles(this.getExamplesTopic());
        }
    }

    private String getStatsTopic() {
        return this.getOutputTopic(STATS);
    }

    private String getExamplesTopic() {
        return this.getOutputTopic(EXAMPLES);
    }

}
