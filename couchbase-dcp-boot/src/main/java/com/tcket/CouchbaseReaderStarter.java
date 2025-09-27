package com.tcket;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class CouchbaseReaderStarter implements CommandLineRunner {
    
    @Autowired
    private CouchbaseReader couchbaseReader;
    
    @Override
    public void run(String... args) throws Exception {
        couchbaseReader.start();
    }
}