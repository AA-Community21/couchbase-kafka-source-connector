# Couchbase DCP Multi-Module Project

This is a multi-module Maven project for Couchbase Database Change Protocol (DCP) integration with Kafka.

## Project Structure

```
couchbase-dcp-parent/
├── couchbase-dcp-core/          # Core models and interfaces
│   ├── CouchbaseChangeData.java
│   └── CouchbaseChangeSerializer.java
├── couchbase-dcp-reader/        # Couchbase DCP reader functionality
│   └── CouchbaseReader.java
├── couchbase-dcp-kafka/         # Kafka integration
│   ├── KafkaConfig.java
│   └── KafkaProducerService.java
└── couchbase-dcp-app/           # Main Spring Boot application
    ├── App.java
    ├── CBConfig.java
    ├── CouchbaseReaderStarter.java
    └── application.yml
```

## Modules

### couchbase-dcp-core
Contains shared models and serializers:
- `CouchbaseChangeData`: Data model for Couchbase changes
- `CouchbaseChangeSerializer`: Kafka serializer for change data

### couchbase-dcp-reader
Contains Couchbase DCP functionality:
- `CouchbaseReader`: Reads changes from Couchbase using DCP

### couchbase-dcp-kafka
Contains Kafka integration:
- `KafkaConfig`: Kafka producer/consumer configuration
- `KafkaProducerService`: Service for serializing data

### couchbase-dcp-app
Main Spring Boot application that ties everything together:
- `App`: Main application class
- `CBConfig`: Couchbase configuration
- `CouchbaseReaderStarter`: Starts the DCP reader on application startup

## Building

To build all modules:
```bash
mvn clean install
```

To build a specific module:
```bash
mvn clean install -pl couchbase-dcp-app
```

## Running

Run the main application:
```bash
cd couchbase-dcp-app
mvn spring-boot:run
```

## Configuration

Update `couchbase-dcp-app/src/main/resources/application.yml` with your:
- Couchbase connection details
- Kafka connection details