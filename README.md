<img src="https://github.com/StanfordBioinformatics/Swarm/blob/master/Swarm-logo.png" width="200" align="right">

# Swarm: A federated Cloud Framework for Large-scale Variant Analysis

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
```

Information about service endpoints are in [API.md](API.md).

More details about installing and running are in [Using.md](doc/Using.md).

Details about connecting to data sources are in [Data.md](doc/Data.md).

Details about future extensions are in [Extending.md](doc/Extending.md).

Details about Federated Swarm extensions are in [README.md](federated/README.md).

* Logo Credit: Camille Berry
