package app.dao.client;

import app.AppLogging;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.apache.logging.log4j.Logger;

import javax.validation.ValidationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class S3UploadStream extends OutputStream {
    private Logger log = AppLogging.getLogger(S3UploadStream.class);
    final static int defaultMultipartPartitionSize = 8 * 1024 * 1024; // 8 MiB
    private S3Client client;
    private String s3Url;
    private AmazonS3 s3;

    private OutputInputStream s3UploadStream;
    private Thread uploadThread;

    /**
     * This class creates an OutputStream to a S3 url, if an object at
     * that url does not already exist.
     * @param client
     * @param s3Url
     */
    public S3UploadStream(S3Client client, String s3Url) {
        this(client, s3Url, defaultMultipartPartitionSize);
    }

    private void multipartPartitionSizeValid(int size) {
        final int max = 4 * 1024 * 1024 * 1024;
        final int min = 1 * 1024 * 1024;
        if (size > max) {
            throw new ValidationException("partition size was over max: " + max);
        }
        if (size < min) {
            throw new ValidationException("partition size was under min: " + min);
        }
        double megaBytes = size / 1024 / 1024;
        if (megaBytes % 2.0 == 0.0) {
            // whole even value number of megabytes
            int megaBytesInt = (int) megaBytes;
            if ((megaBytesInt & (megaBytesInt - 1)) == 0) {
                // has single bit set, so is a power of two
                log.debug("Partition size requested is valid");
                return;
            }
        }
        throw new ValidationException("Partition size was not a positive power of two multiple of a MiB");
    }

    public S3UploadStream(S3Client client, String s3Url, int multipartPartitionSize) {
        log.debug("Constructing new upload stream to: " + s3Url);
        // https://docs.aws.amazon.com/amazonglacier/latest/dev/api-multipart-initiate-upload.html
        // max part size = 4GiB, min = 1 MiB
        this.client = client;
        this.s3Url = s3Url;
        this.s3 = client.getS3();

        S3PathParser parser = new S3PathParser("s3://", s3Url);

        if (client.doesS3ObjectExist(s3Url)) {
            throw new RuntimeException("S3 object already exists: " + parser.bucket + "/" + parser.objectPath);
        }

        log.debug("Creating new OutputInputStream");
        this.s3UploadStream = new OutputInputStream();

        log.debug("Creating new object in bucket");
        Runnable r = new Runnable() {
            @Override
            public void run() {
                ObjectMetadata objectMetadata = new ObjectMetadata();
                InitiateMultipartUploadRequest initiateMultipartUploadRequest =
                        new InitiateMultipartUploadRequest(
                                parser.bucket,
                                parser.objectPath,
                                objectMetadata);
                log.debug("Initiating multipart upload");
                InitiateMultipartUploadResult initiateMultipartUploadResult =
                        s3.initiateMultipartUpload(initiateMultipartUploadRequest);
                String uploadId = initiateMultipartUploadResult.getUploadId();
                log.info("New multipart upload id: " + uploadId);
                int partSize = multipartPartitionSize;
                byte[] partBuffer = new byte[partSize];
                int readSize = -1;
                int partNumber = 1;
                List<PartETag> partETags = new ArrayList<PartETag>();
                try {
                    while ((readSize = s3UploadStream.read(partBuffer)) != -1) {
                        // got a new buffer
                        // create input stream for the byte array for the part
                        ByteArrayInputStream bufferInputStream = new ByteArrayInputStream(partBuffer, 0, readSize);
                        UploadPartRequest uploadPartRequest = new UploadPartRequest()
                                .withBucketName(parser.bucket)
                                .withKey(parser.objectPath)
                                .withUploadId(uploadId)
                                .withPartNumber(partNumber)
                                .withInputStream(bufferInputStream) // use stream instead of file
                                .withPartSize(readSize);
                        // upload it
                        UploadPartResult uploadPartResult = s3.uploadPart(uploadPartRequest);
                        log.debug("Uploaded part: " + partNumber);
                        partNumber++;
                        // record identifier for the part, for validating in completeMultipartUpload
                        partETags.add(uploadPartResult.getPartETag());
                    }
                    log.debug("Finished uploading parts, issuing complete request");
                    CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                            parser.bucket, parser.objectPath, uploadId, partETags);
                    s3.completeMultipartUpload(completeRequest);
                    log.info("Completed multipart upload");
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }

                // this uploads the file in one shot, which buffers the whole input stream into memory
                /*PutObjectResult putObjectResult = s3.putObject(
                        parser.bucket,
                        parser.objectPath,
                        s3UploadStream,
                        objectMetadata);*/
            }
        };
        uploadThread = new Thread(r);
        uploadThread.start();
        log.info("Started read thread, leaving constructor");
    }

    @Override
    public void close() {
        s3UploadStream.close();
        /*s3UploadStream.setClosed();
        try {
            log.debug("Sending close signal to readQueue");
            s3UploadStream.add(-1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }*/
        log.debug("Closed upload stream");
        try {
            uploadThread.join();
            log.debug("Joined upload thread");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(int i) throws IOException {
        //log.debug(String.format("Writing value to upload stream: %d", i));
        try {
            this.s3UploadStream.add(i);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
