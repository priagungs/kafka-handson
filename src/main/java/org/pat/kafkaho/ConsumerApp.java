package org.pat.kafkaho;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class ConsumerApp {
    public static void main(String[] args) {
        runConsumer();
    }

    private static void runConsumer() {
        String topic = "stream-con";
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer(topic);
        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            consumerRecords.forEach(record -> {
                System.out.println("Record value : " + record.value());
                System.out.println("Record partition : " + record.partition());
                System.out.println("Record Offset : " + record.offset());
                System.out.println();
            });
            consumer.commitAsync();
        }
    }
}
