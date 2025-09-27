package com.tcket;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class CBConfig {

    @Value("${spring.couchbase.connection-string}")
    private String connectionString;

    @Value("${spring.couchbase.username}")
    private String username;

    @Value("${spring.couchbase.password}")
    private String password;

    private String bucketName = "abhiii";

    private String collectionName = "UserService.UserService";
    
    @Bean
    public CouchbaseReader getCouchbaseReader() {
        Map<String, String > props = new HashMap<>();
        props.put("couchbase.hosts", connectionString);
        props.put("couchbase.username", username);
        props.put("couchbase.password", password);
        props.put("couchbase.bucket", bucketName);
        props.put("couchbase.collection", collectionName);
        return new CouchbaseReader(props);
    }
}