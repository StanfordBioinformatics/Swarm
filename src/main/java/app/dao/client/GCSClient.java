package app.dao.client;

import app.AppLogging;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static app.dao.client.StringUtils.ensureTrailingSlash;

public class GCSClient {
    private Logger log = AppLogging.getLogger(GCSClient.class);

    private Storage storage;

    public GCSClient(String credentialFilePath) {
        StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();
        try {
            storageOptionsBuilder.setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(credentialFilePath)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.storage = storageOptionsBuilder.build().getService();
    }

    public Storage getStorage() {
        return this.storage;
    }

    public boolean doesGCSObjectExist(BlobId blobId) {
        return storage.get(blobId).exists();
    }

    public boolean doesGCSObjectExist(String url) {
        return doesGCSObjectExist(urlToBlobId(url));
    }

    public BlobId urlToBlobId(String url) {
        S3PathParser parser = new S3PathParser("gs://", url);
        return BlobId.of(parser.bucket, parser.objectPath);
    }

    public String blobIdToString(BlobId blobId) {
        return String.format("gs://%s/%s", blobId.getBucket(), blobId.getName());
    }

    /**
     * Opens an InputStream to the blobId. blobId must be a valid GCS url
     * to an existing object readable with the loaded client credentials.
     * @param blobId
     */
    public InputStream getInputStream(BlobId blobId) throws FileNotFoundException {
        Blob blob = storage.get(blobId);
        if (blob == null) {
            throw new FileNotFoundException(blobId.toString());
        }
        ReadChannel readChannel = blob.reader();
        return Channels.newInputStream(readChannel);
    }

    /**
     * Parses url into BlobId, delegates to GCSClient#getInputStream(BlobId)
     * @param url
     */
    public InputStream getInputStream(String url) throws FileNotFoundException {
        return getInputStream(urlToBlobId(url));
    }

    public List<Blob> listDirectoryBlobs(String directoryUrl) {
        directoryUrl = ensureTrailingSlash(directoryUrl);
        log.debug("listDirectoryBlobs(" + directoryUrl + ")");
        //S3PathParser parser = new S3PathParser("gs://", directoryUrl);
        //parser.objectPath = ensureTrailingSlash(parser.objectPath);
        // ensure this is actually a directory
        BlobId blobId = urlToBlobId(directoryUrl);
        //log.info("Checking whether " + directoryUrl + " is a directory");
        /*Storage.BlobGetOption bgo = Storage.BlobGetOption.
        if (!storage.get(blobId).isDirectory()) {
            throw new IllegalArgumentException("Directory listing failed, url was not a gs directory: " + directoryUrl);
        } else {
            log.info(directoryUrl + " is a directory");
        }*/

        // Option for restricting listing to a certain object key prefix.
        // A bucket directory is simply a prefix containing slashes.
        List<Blob> results = new ArrayList<>();
        Storage.BlobListOption blobListOption = Storage.BlobListOption.prefix(blobId.getName());
        Page<Blob> page = storage.list(blobId.getBucket(), blobListOption);
        while (true) {
            log.info("Iterating over bucket: " + blobId.getBucket() + " prefix: " + blobId.getName());
            for (Blob blob : page.iterateAll()) {
                log.debug(blobId.getBucket() + "/" + blobId.getName() + " contains: " + blob.getName());
                results.add(blob);
            }
            if (!page.hasNextPage()) {
                log.debug("No more entry pages");
                break;
            } else {
                log.debug("Loading next page of directory entries");
                page = page.getNextPage();
            }
        }
        return results;
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

    public List<BlobId> listDirectory(String directoryUrl) {
        List<BlobId> results = new ArrayList<>();
        for (Blob blob : listDirectoryBlobs(directoryUrl)) {
            results.add(blob.getBlobId());
        }
        return results;
    }

    public long getDirectorySize(String directoryUrl) {
        List<BlobId> directoryEntries = listDirectory(directoryUrl);
        // if the directory is more than 8 exabytes, this will overflow
        long totalSize = 0;
        for (BlobId blobId : directoryEntries) {
            totalSize += getObjectSize(blobId);
        }
        log.debug(String.format("getDirectorySize(%s): %d", directoryUrl, totalSize));
        return totalSize;
    }

    public long getObjectSize(BlobId blobId) {
        return storage.get(blobId).getSize();
    }
}
