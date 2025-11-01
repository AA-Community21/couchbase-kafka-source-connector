package com.tcket;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerService.class);

    @Autowired
    private KafkaProducer<byte[], byte[]> producer;

    @Autowired
    private KafkaAdmin kafkaAdmin;

    private static final String TOPIC = "couchbase-changes";

    public void sendCouchbaseChange(String key, CouchbaseChangeData content) {
        try {
            byte[] keyBytes = key.getBytes();
            byte[] valueBytes = content.toBytes();
            kafkaAdmin.createTopic(TOPIC, 3, (short)3);
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(TOPIC, keyBytes, valueBytes);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOGGER.error("Failed to send message to Kafka: {}", exception.getMessage());
                } else {
                    LOGGER.debug("Message sent to topic {} partition {} offset {}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            LOGGER.error("Error sending message to Kafka", e);
        }
    }
}