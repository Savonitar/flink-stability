# Flink Stability Testing Framework

A framework for validating Apache Flink's behavior during various failure scenarios, including rescaling, restarts, network partitions, and version upgrades. The framework ensures data consistency in Kafka by verifying that data is processed exactly once, without losses or duplicates.

## Features

###
Core Capabilities
- ✅Multi-version Flink support(e.g., testing 1.18 → 1.19upgrades)
- ✅Failure scenario testing: 
- TaskManager/JobManager stops
- Network partitions
- Job rescaling
- ✅Savepoint management:
- Stop with savepoint
- Restart from savepoint
- ✅Exactly-once processing validation

###
Kafka Integration
- ✅Automated Kafka setup using Testcontainers
- ✅Input/ output topic management
- ✅Data validation:
- ✅No duplicates
- ✅No data loss

## Architecture

### Configuration
Scenarios are defined in YAML format.

### How to use
mvn clean install && mvn exec:java -pl cli -Dexec.args="run --scenario scenarios/example.yaml"