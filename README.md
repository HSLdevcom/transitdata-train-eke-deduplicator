[![Build Status](https://travis-ci.org/HSLdevcom/transitdata-train-eke-deduplicator.svg?branch=master)](https://travis-ci.org/HSLdevcom/transitdata-train-eke-deduplicator)

# Transitdata-train-eke-deduplicator

## Description

Application for de-duplicating EKE unit Messages read from single/multiple Pulsar topics.
Writes de-duplicated output to another Pulsar topic.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

### Locally

- ```mvn compile```  
- ```mvn package```  

### Docker image

- Run [this script](build-image.sh) to build the Docker image


## Running

Requirements:
- Local Pulsar Cluster
  - By default uses localhost, override host in PULSAR_HOST if needed.
    - Tip: f.ex if running inside Docker in OSX set `PULSAR_HOST=host.docker.internal` to connect to the parent machine
  - You can use [this script](https://github.com/HSLdevcom/transitdata/blob/master/bin/pulsar/pulsar-up.sh) to launch it as Docker container

Launch Docker container with

```docker-compose -f compose-config-file.yml up <service-name>```   
