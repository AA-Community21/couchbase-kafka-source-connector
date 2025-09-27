package com.tcket;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.highlevel.*;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class CouchbaseReader implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseReader.class);

    private final Client client;
    private final CountDownLatch connectionAttemptComplete = new CountDownLatch(1);
    private volatile boolean connected;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    public CouchbaseReader(Map<String, String> props) {
        String connectionString = props.get("couchbase.hosts");
        String username = props.get("couchbase.username");
        String password = props.get("couchbase.password");
        String bucketName = props.get("couchbase.bucket");
        
        client = Client.builder()
                .seedNodes(connectionString)
                .credentials(username, password)
                .bucket(bucketName)
                .collectionsAware(true)
                .build();

       registerListener();
    }

    public void start() {
        new Thread(this).start();
    }
    
    public void stop() {
        shutdown();
    }
    
    public List<CouchbaseChangeData> pollChanges() {
        List<CouchbaseChangeData> changes = new ArrayList<>();
        return changes;
    }

    private void registerListener() {
        client.nonBlockingListener(new DatabaseChangeListener() {
            @Override
            public void onMutation(Mutation mutation) {
                handleChange(mutation);
            }

            @Override
            public void onDeletion(Deletion deletion) {
                handleChange(deletion);
            }

            @Override
            public void onFailure(StreamFailure failure) {
                Throwable cause = failure.getCause();
                String errorMsg = cause != null ? cause.toString() : "Unknown cause";
                LOGGER.error("Stream failure occurred: {}", errorMsg);
            }

            @Override
            public void onStreamEnd(StreamEnd streamEnd) {
                LOGGER.info("Stream ended: {}", streamEnd.getReason());
            }

            private void handleChange(DocumentChange change) {
                try {
                    String content = new String(change.getContent(), StandardCharsets.UTF_8);
                    CollectionsManifest.CollectionInfo collectionInfo = change.getCollection();

                    LOGGER.info("Collection ID: {}, Collection Name: {}, Scope: {}",
                            collectionInfo.id(), collectionInfo.name(), collectionInfo.scope());

                    CouchbaseChangeData changeData = new CouchbaseChangeData(
                        change.getKey(), content, collectionInfo.name(), change.getTimestamp());
                    kafkaProducerService.sendCouchbaseChange(change.getKey(), content);

                    String sanitizedContent = content.replaceAll("[\r\n\t]", "_");
                    LOGGER.info("Received DCP change {}, {}, {}, {} : {}", 
                            change.getKey(), change.getCollection(), change.getTimestamp(), 
                            change.getVbucket(), sanitizedContent);

                } catch (RuntimeException ex) {
                    LOGGER.error("Runtime error processing DCP change", ex);
                } catch (Exception ex) {
                    LOGGER.error("Unexpected error processing DCP change", ex);
                }
            }
        });
    }

    @Override
    public void run() {
        try {
            LOGGER.info("Connecting to Couchbase...");
            client.connect().block();
            connected = true;
            LOGGER.info("Connected. Initializing state...");

            client.initializeState(StreamFrom.BEGINNING, StreamTo.INFINITY).block();
            LOGGER.info("State initialized. Starting streaming...");

            client.startStreaming().block();
            LOGGER.info("DCP streaming started successfully.");
        } catch (RuntimeException ex) {
            LOGGER.error("Runtime error during Couchbase DCP setup", ex);
        } catch (Exception ex) {
            LOGGER.error("Unexpected error during Couchbase DCP setup", ex);
        } finally {
            connectionAttemptComplete.countDown();
        }
    }

    public void shutdown() {
        try {
            if (connected) {
                LOGGER.info("Disconnecting Couchbase DCP Client...");
                client.disconnect().block();
                connected = false;
                LOGGER.info("Disconnected successfully.");
            }
        } catch (RuntimeException e) {
            LOGGER.error("Runtime error during disconnect", e);
        } catch (Exception e) {
            LOGGER.error("Unexpected error during disconnect", e);
        }
    }
}