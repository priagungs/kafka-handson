package org.pat.kafkaho;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ProducerCreator {
    public static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        try {
            props.load(new FileReader(new File("kafka.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new KafkaProducer<>(props);
    }
}
