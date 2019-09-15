package org.pat.kafkaho;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class ConsumerApp {
    public static void main(String[] args) {
        runConsumer();
    }

    private static void runConsumer() {
        String topic = "topichuyu";
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer(topic);
        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            //print each record.
            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });
            // commits the offset of record to broker.
            consumer.commitAsync();
        }
    }
}
