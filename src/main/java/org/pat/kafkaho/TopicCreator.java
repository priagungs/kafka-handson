package org.pat.kafkaho;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicCreator {
    public static void createTopic(String topic, int partition) {
        Properties properties = new Properties();
        try {
            properties.load(new FileReader(new File("kafka.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        AdminClient adminClient = AdminClient.create(properties);
        NewTopic newTopic = new NewTopic(topic, partition, (short)1); //new NewTopic(topicName, numPartitions, replicationFactor)

        try {
            adminClient.createTopics(Collections.singletonList(newTopic));
            System.out.println(adminClient.listTopics().listings().get().iterator().next().name());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        adminClient.close();
    }
}
