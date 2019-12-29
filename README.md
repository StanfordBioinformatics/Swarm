# Swarm

This is a Spring Boot server that queries genomic variants from Google BigQuery and Amazon Athena.

## Pre-requisites

- Obtain a GCP Service Account key file in JSON format
- Obtain an AWS Access Key/Secret. Place in top level directory file `aws.properties` with names `accessKey` and `secretKey`. Also include `outputLocationRoot`, which is the s3 bucket url including optional directory.

## Build

Run the following once to install the Simba Athena JDBC driver into the local maven cache.
```
$ bash lib/install-lib.sh
```

Run the following to build.
```
$ mvn package
```

## Run

Maven builds a jar and puts it in `target/` directory.  It defaults to port 8080, which must be unbound prior to running.
```
$ java -jar target/swarm-1.0-SNAPSHOT.jar
