# Using Swarm


## Configuration

### Authentication

One part of configuring this service is to enable it to communicate with the necessary cloud services. On AWS an IAM identity should be created/configured with read and write access to Athena and S3, and on GCP a service account should be created/configured with read and write access to BigQuery and Google Cloud Storage.

GCP provides credential downloads for service accounts in the form of a `.json` file. This should be saved to a local file in the top level of the swarm directory, called `gcp.json`.

AWS does not provide such a consistent credential file format for download and loading by its libraries, and we do not have an intention to parse boto files either. Swarm will instead look for a simple key-value property file called `aws.properties` in the top level working directory, which should contain properties for the AWS secret key identifier and secret.

The file `aws.properties` should have at least the following properties:
```
accessKey=<aws-key-name>
secretKey=<aws-key-secret>
outputLocationRoot=s3://<bucket-name-with-write-permission>
```

## Running

### Standalone

Swarm's query and transfer service is a Java-based Spring Boot REST API. To use, Java version 11+ and Maven should be installed.

To build, use the command `mvn package`. This will build a `.jar` file in the `target` directory, which can be run with `java -jar <jar-file>`.


### Docker

A Dockerfile is provided which bundles the entire working directory into the image.

**NOTE**: Since it includes credentials, this should **NOT** be published to a public registry.

```
$ docker build . -t swarm
$ docker run -p 80:80 swarm
```
