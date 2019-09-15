package org.pat.kafkaho;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerCreator {
    public static Consumer<Long, String> createConsumer(String... topics) {
        Properties props = new Properties();
        try {
            props.load(new FileReader(new File("kafka.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topics));
        return consumer;
    }
}
