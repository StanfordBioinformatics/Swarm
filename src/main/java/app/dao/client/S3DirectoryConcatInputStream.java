package app.dao.client;

import app.AppLogging;
import com.amazonaws.services.s3.model.S3ObjectId;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * This class is used to download all files in an S3 directory, assuming all contents
 * are GZIP compressed, decompress them, concat them, and provide an InputStream to the result.
 * <br>
 * Uses host temp directory, and files saved there are deleted when the stream is closed.
 */
public class S3DirectoryGzipConcatInputStream extends InputStream {
    private Logger log = AppLogging.getLogger(S3DirectoryGzipConcatInputStream.class);

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
     * @param s3DirectoryUrl
     */
    public S3DirectoryGzipConcatInputStream(S3Client client, String s3DirectoryUrl) throws IOException {
        outFilePath = Files.createTempFile(
                "athena-table-",
                ".tmp");
        log.info("Using temp file: " + outFilePath.toString());
        OutputStream fileOutputStream = new FileOutputStream(outFilePath.toFile());

        // Download all directory contents (still gzipped)
        List<S3ObjectId> directoryObjects = client.listDirectory(s3DirectoryUrl);
        for (S3ObjectId objectId : directoryObjects) {
            InputStream is = client.getInputStream(objectId);
            GZIPInputStream gunzipStream = new GZIPInputStream(is);
            gunzipStream.transferTo(fileOutputStream);
            gunzipStream.close(); // theoretically closes 'is' as well
            is.close();
            String s3Path = "s3://" + objectId.getBucket() + "/" + objectId.getKey();
            log.debug("Downloaded and decompressed s3 file: " + s3Path);
        }
        fileOutputStream.flush();
        fileOutputStream.close();

        outFileInputStream = new FileInputStream(outFilePath.toFile());
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
