package com.tcket;

import java.time.Instant;

public class CouchbaseChangeData {
    private String key;
    private String content;
    private String collection;
    private Instant timestamp;
    private int vbucket;
    
    public CouchbaseChangeData() {}
    
    public CouchbaseChangeData(String key, String content, String collection, Instant timestamp, int vbucket) {
        this.key = key;
        this.content = content;
        this.collection = collection;
        this.timestamp = timestamp;
        this.vbucket = vbucket;
    }
    
    public CouchbaseChangeData(String key, String content, String collection, Instant timestamp) {
        this.key = key;
        this.content = content;
        this.collection = collection;
        this.timestamp = timestamp;
    }
    
    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }
    
    public String getContent() { return content; }
    public String getValue() { return content; } // Alias for Kafka Connect
    public void setContent(String content) { this.content = content; }
    
    public String getCollection() { return collection; }
    public String getBucket() { return collection; } // Alias for bucket name
    public void setCollection(String collection) { this.collection = collection; }
    
    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
    
    public int getVbucket() { return vbucket; }
    public long getSequence() { return System.currentTimeMillis(); } // Simple sequence number
    public void setVbucket(int vbucket) { this.vbucket = vbucket; }
}