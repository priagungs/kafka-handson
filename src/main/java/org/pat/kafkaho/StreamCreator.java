package org.pat.kafkaho;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class StreamCreator {
    public static KafkaStreams createStream(StreamsBuilder builder) {
        Properties props = new Properties();
        try {
            props.load(new FileReader(new File("kafka.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new KafkaStreams(builder.build(), props);
    }
}
