package com.tcket;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class CouchbaseChangeSerializer implements Serializer<CouchbaseChangeData> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public byte[] serialize(String topic, CouchbaseChangeData data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing CouchbaseChangeData", e);
        }
    }
}