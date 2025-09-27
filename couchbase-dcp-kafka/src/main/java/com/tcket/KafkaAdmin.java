package com.tcket;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaAdmin {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAdmin.class);

    @Autowired
    private AdminClient kafkaAdmin;

    @Autowired
    private KafkaConsumer<String, String> kafkaConsumer;

    private Set<String> topics = new HashSet<>();

    @PostConstruct
    private void init() {
        listTopics();
        logger.info("Existing topics: " + topics);
    }


    public void createTopic(String topicName, int numPartitions, short replicationFactor) throws ExecutionException, InterruptedException {
        if (topics.contains(topicName)) {
            return;
        }
        try {
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
            if (!topics.contains(topicName)) {
                topics.add(topicName);
                CreateTopicsResult result = kafkaAdmin.createTopics(Collections.singletonList(newTopic));
                result.all().get();
            }
        } catch (Exception e) {
            logger.error("Error creating topic: " + e.getMessage());
            throw e;
        }
    }

    public void listTopics() {
        try {
            ListTopicsOptions topicsOptions = new ListTopicsOptions()
                    .timeoutMs(30000);
            ListTopicsResult topicResult = kafkaAdmin.listTopics(topicsOptions);
            Set<String> currentTopics = topicResult.names().get();
            topics.addAll(currentTopics);
        } catch (Exception e) {
            logger.error("Error listing topics: " + e.getMessage());
        }
    }

    @PreDestroy
    public void close() throws ExecutionException, InterruptedException {
        if (kafkaAdmin != null) {
            try {
                Instant start = Instant.now();
                DescribeTopicsResult topicDescription = kafkaAdmin.describeTopics(topics);
                Map<String, TopicDescription> tpds = topicDescription.all().get();

                List<TopicPartition> allPartitions = tpds.values().stream()
                    .flatMap(tpd -> tpd.partitions().stream()
                        .map(partitionInfo -> new TopicPartition(tpd.name(), partitionInfo.partition())))
                    .toList();

                Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(allPartitions);
                
                Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
                for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
                    if (entry.getValue() > 0) {
                        recordsToDelete.put(entry.getKey(), RecordsToDelete.beforeOffset(entry.getValue()));
                    }
                }

                if (!recordsToDelete.isEmpty()) {
                    DeleteRecordsResult deleteResult = kafkaAdmin.deleteRecords(recordsToDelete);
                    deleteResult.all().get();
                    logger.info("Successfully deleted records from {} partitions", recordsToDelete.size());
                }
                Instant end = Instant.now();
                logger.info("Time taken to delete records: " + (end.toEpochMilli() - start.toEpochMilli()) + " ms");
            } catch (Exception e) {
                logger.error("Error deleting records: " + e.getMessage(), e);
            } finally {
                kafkaAdmin.close();
            }
        }
    }

}
