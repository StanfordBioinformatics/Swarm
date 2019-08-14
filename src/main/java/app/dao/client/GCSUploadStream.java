package app.dao.client;

import app.AppLogging;
import com.google.cloud.storage.*;
import org.apache.logging.log4j.Logger;

import java.io.*;

// TODO move some of these methods into GCSClient
// TODO this could probably be updated to use the following method:
// https://cloud.google.com/appengine/docs/standard/java/googlecloudstorageclient/read-write-to-cloud-storage
// https://github.com/GoogleCloudPlatform/appengine-gcs-client/blob/d11078331ecd915d753c886e96a80133599f3f98/java/example/src/main/java/com/google/appengine/demos/GcsExampleServlet.java#L93
public class GCSUploadStream extends OutputStream {
    private Logger log = AppLogging.getLogger(GCSUploadStream.class);

    private GCSClient client;
    private String gcsUrl;
    private BlobInfo blobInfo;
    private Storage storage; // GCP storage client class from Google
    private Bucket bucket; // handle to the bucket
    private Blob blob; // after file is uploaded to GCS, this contains a handle to the object
    private OutputInputStream gcsUploadInputStream;

    private Thread uploadThread;

    /**
     * This class creates an OutputStream to a GCS url, if an object at
     * that url does not already exist.
     * @param client
     * @param gcsUrl
     */
    public GCSUploadStream(GCSClient client, String gcsUrl) {
        log.debug("Constructing new upload stream to: " + gcsUrl);
        this.client = client;
        this.gcsUrl = gcsUrl;
        S3PathParser parser = new S3PathParser("gs://", gcsUrl);
        // similar to S3ObjectId, container object for info about an object in a bucket.
        this.blobInfo = Blob.newBuilder(parser.bucket, parser.objectPath)
                .build();

        this.storage = client.getStorage(); //StorageOptions.getDefaultInstance().getService();
        this.bucket = storage.get(parser.bucket); // checked for validity
        if (blobExists(parser.objectPath)) {
            throw new RuntimeException("GCS object already exists: " + parser.bucket + "/" + parser.objectPath);
        }

        log.debug("Creating new GCSUploadInputStream");
        //this.gcsUploadInputStream = new GCSUploadInputStream();
        this.gcsUploadInputStream = new OutputInputStream();

        log.debug("Creating object in bucket");
        // Bucket.create is blocking so must run in separate thread
        Runnable r = new Runnable() {
            @Override
            public void run() {
                blob = bucket.create(parser.objectPath, gcsUploadInputStream);
                log.info("Created object");
            }
        };
        uploadThread = new Thread(r);
        // this thread is automatically joined back to the current context when the stream is closed
        uploadThread.start();
        log.info("Started read thread, leaving constructor");

    }

    private boolean blobExists(String blobName) {
        Blob blob = this.bucket.get(blobName);
        return blob != null;
    }

    /**
     * It's extremely important that this is called.
     */
    @Override
    public void close() {
        gcsUploadInputStream.close();
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
        try {
            this.gcsUploadInputStream.add(i);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
