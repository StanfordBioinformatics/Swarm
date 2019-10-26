package app.dao.client;

import app.AppLogging;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.logging.log4j.Logger;

import javax.validation.ValidationException;
import java.io.*;
import java.util.*;

import static app.dao.client.StringUtils.ensureTrailingSlash;

public class S3Client {
    private Logger log = AppLogging.getLogger(S3Client.class);

    // prop names
    private final String PROP_NAME_KEY_ID = "accessKey";
    private final String PROP_NAME_SECRET_KEY = "secretKey";
    private final String PROP_NAME_OUTPUT_LOCATION = "outputLocationRoot";

    private Properties configuration;

    private AmazonS3 s3;

    /**
     * This class must know the region being used up front, unlike GCSClient.
     * <br>
     * It also cannot be modified at a later time, so if using multiple regions,
     * multiple clients must be constructed.
     * @param credentialPath
     * @param region
     */
    public S3Client(String credentialPath, Region region) {
        // load configuration
        Properties credProperties = new Properties();
        try {
            log.info("loading credential file: " + credentialPath);
            credProperties.load(new FileInputStream(credentialPath));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        // validate loaded properties
        // SystemPropertiesCredentialsProvider looks for aws.accessKeyId and aws.secretKey
        if (!(credProperties.containsKey(PROP_NAME_KEY_ID)
                && credProperties.containsKey(PROP_NAME_SECRET_KEY)
                && credProperties.containsKey(PROP_NAME_OUTPUT_LOCATION)
        )) {
            log.error("credential file was missing required keys");
            throw new RuntimeException(String.format(
                    "Credentials loaded from " + credentialPath
                            + " were missing %s or %s or %s",
                    PROP_NAME_KEY_ID, PROP_NAME_SECRET_KEY, PROP_NAME_OUTPUT_LOCATION));
        }

        this.configuration = credProperties;

        // set the aws key properties
        //System.setProperty(PROP_NAME_KEY_ID, credProperties.getProperty(PROP_NAME_KEY_ID));
        //System.setProperty(PROP_NAME_SECRET_KEY, credProperties.getProperty(PROP_NAME_SECRET_KEY));

        AWSCredentialsProvider credProvider = new PropertiesFileCredentialsProvider(credentialPath);
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.setRegion(region.getName());
        builder.setCredentials(credProvider);
        this.s3 = builder.build();
    }

    public List<S3ObjectId> listDirectory(String directoryUrl) {
        log.debug("listDirectoryBlobs(" + directoryUrl + ")");
        S3PathParser parser = new S3PathParser(directoryUrl);
        parser.objectPath = ensureTrailingSlash(parser.objectPath);

        ListObjectsV2Request listRequest = new ListObjectsV2Request()
                .withBucketName(parser.bucket)
                .withPrefix(parser.objectPath);
        ListObjectsV2Result listResult = s3.listObjectsV2(listRequest);
        List<S3ObjectId> resultIds = new ArrayList<S3ObjectId>();
        List<S3ObjectSummary> summaries = listResult.getObjectSummaries();
        for (S3ObjectSummary summary : summaries) {
            S3ObjectId id = new S3ObjectId(summary.getBucketName(), summary.getKey());
            resultIds.add(id);
        }
        return resultIds;
    }

    public String s3ObjectIdToString(S3ObjectId objectId) {
        return String.format("s3://%s/%s", objectId.getBucket(), objectId.getKey());
    }

    public long getDirectorySize(String directoryUrl) {
        List<S3ObjectId> directoryEntries = listDirectory(directoryUrl);
        long totalSize = 0;
        for (S3ObjectId entry : directoryEntries) {
            totalSize += getObjectSize(entry);
        }
        log.debug(String.format("getDirectorySize(%s): %d", directoryUrl, totalSize));
        return totalSize;
    }

    public long getObjectSize(S3ObjectId objectId) {
        ObjectMetadata metadata = s3.getObjectMetadata(objectId.getBucket(), objectId.getKey());
        // Instance Length is the size of the object in S3.
        // Content Length is length of this particular request.
        return metadata.getInstanceLength();
    }

    public boolean doesS3ObjectExist(String url) {
        S3PathParser parser = new S3PathParser(url);
        String bucket = parser.bucket;
        String objectPath = parser.objectPath;

        if (!s3.doesBucketExistV2(bucket)) {
            log.warn("Bucket " + bucket + " does not exist");
            return false;
        }

        if (!s3.doesObjectExist(bucket, objectPath)) {
            log.debug("Object key " + objectPath + " does not exist in bucket " + bucket);
            return false;
        }
        return true;
    }

    public S3ObjectId urlToS3ObjectId(String url) {
        S3PathParser parser = new S3PathParser(url);
        S3ObjectIdBuilder builder = new S3ObjectIdBuilder()
                .withBucket(parser.bucket)
                .withKey(parser.objectPath);
        return builder.build();
    }

    public InputStream getInputStream(String url) {
//        if (!doesS3ObjectExist(url)) {
//            throw new IllegalArgumentException("Could not recognize url as an S3 url: " + url);
//        }
        S3ObjectId id = urlToS3ObjectId(url);
        return getInputStream(id);
    }

    public InputStream getInputStream(S3ObjectId objectId) {
        GetObjectRequest getObjectRequest = new GetObjectRequest(objectId);
        S3Object object = s3.getObject(getObjectRequest);
        S3ObjectInputStream contentStream = object.getObjectContent();
        BufferedInputStream bufferedContentStream = new BufferedInputStream(contentStream);
        return bufferedContentStream;
    }

    public String getFirstLineOfFile(String objectUrl) throws IOException {
        try (InputStream is = this.getInputStream(objectUrl);
             Scanner scany = new Scanner(is);)
        {
            if (!scany.hasNextLine()) {
                throw new IOException("Failed to read line from GCS object: " + objectUrl);
            }
            String s = scany.nextLine();
            return s;
        }
    }

    public AmazonS3 getS3() {
        return this.s3;
    }
}
