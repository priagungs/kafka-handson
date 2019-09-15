package org.pat.kafkaho;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class ProducerApp {
    public static void main(String[] args) {
        runProducer();
    }

    private static void runProducer() {
        Scanner scanner = new Scanner(System.in);
        String topic = "topichuyu";
//        TopicCreator.createTopic(topic, 1);
        Producer<Long, String> producer = ProducerCreator.createProducer();
        while (true) {
            String value = scanner.nextLine();
            ProducerRecord<Long, String> record = new ProducerRecord<>(topic, value);
            RecordMetadata metadata = null;
            try {
                metadata = producer.send(record).get();
                System.out.println("Record sent to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

        }
    }
}
