package com.tcket;

import java.time.Instant;

public class CouchbaseChangeData {
    private String key;
    private String content;
    private Instant timestamp;
    private int vbucket;
    private String scopeName;
    private String collectionName;
    private String changeType;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public int getVbucket() {
        return vbucket;
    }

    public void setVbucket(int vbucket) {
        this.vbucket = vbucket;
    }

    public String getScopeName() {
        return scopeName;
    }

    public void setScopeName(String scopeName) {
        this.scopeName = scopeName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getChangeType() {
        return changeType;
    }

    public void setChangeType(String changeType) {
        this.changeType = changeType;
    }


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String key;
        private String content;
        private Instant timestamp;
        private int vbucket;
        private String scopeName;
        private String collectionName;
        private String changeType;

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder content(String content) {
            this.content = content;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder vbucket(int vbucket) {
            this.vbucket = vbucket;
            return this;
        }

        public Builder scopeName(String scopeName) {
            this.scopeName = scopeName;
            return this;
        }

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder changeType(String changeType) {
            this.changeType = changeType;
            return this;
        }

        public CouchbaseChangeData build() {
            CouchbaseChangeData data = new CouchbaseChangeData();
            data.key = this.key;
            data.content = this.content;
            data.timestamp = this.timestamp;
            data.vbucket = this.vbucket;
            data.scopeName = this.scopeName;
            data.collectionName = this.collectionName;
            data.changeType = this.changeType;
            return data;
        }
    }


    @Override
    public String toString() {
        return "CouchbaseChangeData{" +
                "key='" + key + '\'' +
                ", content='" + content + '\'' +
                ", timestamp=" + timestamp +
                ", vbucket=" + vbucket +
                ", scopeName='" + scopeName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", changeType='" + changeType + '\'' +
                '}';
    }

    public byte[] toBytes() {
        return toString().getBytes();
    }
}