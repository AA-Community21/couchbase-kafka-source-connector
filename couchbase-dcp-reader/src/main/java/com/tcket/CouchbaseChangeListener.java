package com.tcket;

import com.couchbase.client.dcp.highlevel.*;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CouchbaseChangeListener implements DatabaseChangeListener {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseChangeListener.class);

    public static Map<Long, String> scopeIdToName = new HashMap<>();
    public static Map<Long, String> collectionIdToName = new HashMap<>();
    private static final Set<String> loggedEvents = new HashSet<>();

    @Override
    public void onMutation(Mutation mutation) {
        handleChange(mutation);
    }

    @Override
    public void onDeletion(Deletion deletion) {
        handleChange(deletion);
    }

    @Override
    public void onScopeCreated(ScopeCreated scopeCreated) {
        String eventKey = "scope_created_" + scopeCreated.getScopeId();
        if (loggedEvents.contains(eventKey)) {
            return;
        }
        loggedEvents.add(eventKey);

        scopeIdToName.put(scopeCreated.getScopeId(), scopeCreated.getScopeName());
        LOGGER.info("Scope created: {}", scopeCreated.getScopeName());
    }

    @Override
    public void onScopeDropped(ScopeDropped scopeDropped) {

        String eventKey = "scope_dropped_" + scopeDropped.getScopeId();
        if (loggedEvents.contains(eventKey)) {
            return;
        }
        loggedEvents.add(eventKey);

        String scopeName = scopeIdToName.get(scopeDropped.getScopeId());
        LOGGER.info("Scope dropped: {}", scopeName);
        scopeIdToName.remove(scopeDropped.getScopeId());
    }

    @Override
    public void onCollectionCreated(CollectionCreated collectionCreated) {

        String eventKey = "collection_created_" + collectionCreated.getCollectionId();
        if (loggedEvents.contains(eventKey)) {
            return;
        }
        loggedEvents.add(eventKey);

        String scopeName = scopeIdToName.get(collectionCreated.getScopeId());
        collectionIdToName.put(collectionCreated.getCollectionId(), collectionCreated.getCollectionName());
        LOGGER.info("Collection Created: {}, for the Scope: {}", collectionCreated.getCollectionName(), scopeName);

    }

    @Override
    public void onCollectionDropped(CollectionDropped collectionDropped) {

        String eventKey = "collection_dropped_" + collectionDropped.getCollectionId();
        if (loggedEvents.contains(eventKey)) {
            return;
        }
        loggedEvents.add(eventKey);

        String scopeName = scopeIdToName.get(collectionDropped.getScopeId());
        String collectionName = collectionIdToName.get(collectionDropped.getCollectionId());
        LOGGER.info("Collection dropped: {}, for the Scope: {}", collectionName, scopeName);
        collectionIdToName.remove(collectionDropped.getCollectionId());
    }

    @Override
    public void onStreamEnd(StreamEnd streamEnd) {
        LOGGER.info("Stream ended: {}", streamEnd.getReason());
    }

    @Override
    public void onFailure(StreamFailure streamFailure) {
        Throwable cause = streamFailure.getCause();
        String errorMsg = cause != null ? cause.toString() : "Unknown cause";
        LOGGER.error("Stream failure occurred: {}", errorMsg);

    }


    private void handleChange(DocumentChange change) {
        try {
            String content = new String(change.getContent(), StandardCharsets.UTF_8);
            CollectionsManifest.CollectionInfo collectionInfo = change.getCollection();

            String type = change.isMutation() ? "Mutation" : "Deletion";

            LOGGER.info("Collection ID: {}, Collection Name: {}, Scope: {}",
                    collectionInfo.id(), collectionInfo.name(), collectionInfo.scope());

            CouchbaseChangeData couchbaseChangeData = CouchbaseChangeData.builder()
                    .key(change.getKey())
                    .content(content)
                    .timestamp(change.getTimestamp())
                    .vbucket(change.getVbucket())
                    .scopeName(collectionInfo.scope().name())
                    .collectionName(collectionInfo.name())
                    .changeType(type)
                    .build();

            LOGGER.info("Received DCP change: {}", couchbaseChangeData.toString());

            kafkaProducerService.sendCouchbaseChange(change.getKey(), couchbaseChangeData);

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
}
