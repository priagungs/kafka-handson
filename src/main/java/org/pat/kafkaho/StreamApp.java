package org.pat.kafkaho;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class StreamApp {
    public static void main(String[] args) {
        runStream();
    }

    private static void runStream() {
        String inTopic = "prod-stream";
        String outTopic = "stream-con";
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, String> kstream = builder.stream(inTopic);
        kstream.mapValues(String::toUpperCase).to(outTopic);
        StreamCreator.createStream(builder).start();
    }
}
