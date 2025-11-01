package com.tcket;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.highlevel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.CountDownLatch;
import java.util.Map;

public class CouchbaseReader implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseReader.class);

    private final Client client;
    private final CountDownLatch connectionAttemptComplete = new CountDownLatch(1);
    private volatile boolean connected;

    @Autowired
    private DatabaseChangeListener couchbaseChangeListener;

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

    private void registerListener() {
        client.nonBlockingListener(couchbaseChangeListener);
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

}