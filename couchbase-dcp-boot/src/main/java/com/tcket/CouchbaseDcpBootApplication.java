package com.tcket;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.tcket")
public class CouchbaseDcpBootApplication {
    public static void main(String[] args) {
        SpringApplication.run(CouchbaseDcpBootApplication.class, args);
    }
}