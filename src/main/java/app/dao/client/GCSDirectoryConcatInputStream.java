package app.dao.client;

import app.AppLogging;
import com.google.cloud.storage.BlobId;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * This class is used to download all files in a GCS directory, assuming all contents
 * are GZIP compressed, decompress them, concat them, and provide an InputStream to the result.
 * <br>
 * Uses host temp directory, and files saved there are deleted when the stream is closed.
 */
public class GCSDirectoryConcatInputStream extends InputStream {
    private Logger log = AppLogging.getLogger(GCSDirectoryConcatInputStream.class);

    private Path outFilePath;
    private FileInputStream outFileInputStream;

    /**
     * Very important that the file paths are indeed S3 file objects, not directories.
     * This will download and decompress the filePaths into the host temp directory.
     * <br>
     * Since files are concatenated together, each file should not have its own header
     * if this will interfere with parsing operations on the resulting stream.
     *
     * @param client
     * @param gcsDirectoryUrl
     */
    public GCSDirectoryConcatInputStream(GCSClient client, String gcsDirectoryUrl, boolean isGzip) throws IOException {
        outFilePath = Files.createTempFile(
                "bigquery-table-",
                ".tmp");
        log.info("Using temp file: " + outFilePath.toString());
        OutputStream fileOutputStream = new FileOutputStream(outFilePath.toFile());

        // Download all directory contents (potentially gzipped)
        List<BlobId> directoryObjects = client.listDirectory(gcsDirectoryUrl);
        for (BlobId objectId : directoryObjects) {
            InputStream is = client.getInputStream(objectId);
            if (isGzip) {
                GZIPInputStream gunzipStream = new GZIPInputStream(is);
                gunzipStream.transferTo(fileOutputStream);
                gunzipStream.close(); // theoretically closes 'is' as well
            } else {
                is.transferTo(fileOutputStream);
            }

            is.close();
            String gcsPath = "gs://" + objectId.getBucket() + "/" + objectId.getName();
            log.debug("Downloaded and GCS file: " + gcsPath);
        }
        fileOutputStream.flush();
        fileOutputStream.close();

        outFileInputStream = new FileInputStream(outFilePath.toFile());
    }

    public GCSDirectoryConcatInputStream(GCSClient client, String gcsDirectoryUrl) throws IOException {
        this(client, gcsDirectoryUrl, false);
    }

    @Override
    public void close() throws IOException {
        outFileInputStream.close();
        Files.deleteIfExists(outFilePath);
        log.debug("Deleted file: " + outFilePath.toString());
    }

    @Override
    public int read() throws IOException {
        return outFileInputStream.read();
    }
}
