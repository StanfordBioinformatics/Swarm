package app;

import app.dao.client.*;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.google.cloud.bigquery.Table;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Optional;

import static app.dao.client.StringUtils.getLastNonEmptySegmentOfPath;

public class Main {
    public static void main(String[] args) throws SQLException, IOException {
        /*AthenaClient athenaClient = new AthenaClient(
                "swarm",
                "aws.properties"
        );

        String tableName = "temptable";
        boolean exists = athenaClient.doesTableExist(tableName);
        System.out.println("table " + tableName + " exists: " + exists);

        athenaClient.deleteTable("temptable");*/

        Main.athenaQuery();
    }

    public static void s3Upload() throws IOException {

        String awsCredentialFilePath =
                "aws.properties";
        S3Client client = new S3Client(awsCredentialFilePath, Region.getRegion(Regions.US_WEST_1));
        String fileToUpload = "random-100M.dat";
        String s3Url = "s3://gbsc-gcp-project-annohive-dev-user-krferrit/files/" + fileToUpload;

        FileInputStream fileInputStream = new FileInputStream(fileToUpload);
        byte[] inputBytes = fileInputStream.readAllBytes();
        /*for (int i = 0; i < inputBytes.length; i++) {
            //System.out.printf("%02X", inputBytes[i]);
            byte neg1 = (byte) 0xFF;
            if (inputBytes[i] == (int)-1) {
                System.out.println("input value was (int)-1");
            }
        }*/

        //inputBytes = new byte[]{-1};
        //System.out.printf("%02X\n", inputBytes[0]);

        System.out.println("Read " + inputBytes.length + " bytes from input file");

        S3UploadStream s3FileUploadStream = new S3UploadStream(client, s3Url);
        String fileContent = "Hello World!";
        s3FileUploadStream.write(inputBytes);
        System.out.println("Finished writing to s3FileUploadStream");
        s3FileUploadStream.close();
        System.out.println("Closed s3FileUploadStream");

    }

    public static void gcsUpload() throws IOException {
        String gcpCredentialFilePath =
                "gbsc-gcp-project-annohive-dev-4f980c934a52-defaultserviceaccount.json";
        GCSClient client = new GCSClient(gcpCredentialFilePath);
        String gcsUrl = "gs://gbsc-gcp-project-annohive-dev-user-krferrit/files/newfile1.txt";
        GCSUploadStream gcsUploadStream = new GCSUploadStream(client, gcsUrl);

        String fileContent = "Hello World!";
        gcsUploadStream.write(fileContent.getBytes());
        System.out.println("Finished writing to gcsFileUploadStream");
        gcsUploadStream.close();
        System.out.println("Closed gcsFileUploadStream");
    }

    public static void athenaQuery() throws IOException {
        String databaseName = "swarm";
        String credentialPath = "aws.properties";
        AthenaClient athenaClient = new AthenaClient(databaseName, credentialPath);

        String referenceName = "16";
        Long startPosition = (long) 1000000;
        Long endPosition = (long) startPosition + 100000;
        String referenceBases = "A";
        String alternateBases = "C";
        Double minorAF = null;
        Double minorAFMarginOfErrorPercentage = null;
        String sourceTable = "thousandgenomes_vcf_half2";
        //String destinationDataset = "swarm";
        //String destinationTable = "temptable";
        S3ObjectId resultS3Directory = athenaClient.executeVariantQuery(
                referenceName,
                startPosition,
                endPosition,
                referenceBases,
                alternateBases,
                minorAF,
                minorAFMarginOfErrorPercentage,
                sourceTable,
                Optional.empty(), //Optional.of(destinationDataset),
                Optional.empty(), //Optional.of(destinationTable),
                Optional.of(true));
        S3Client s3Client = new S3Client(credentialPath, Region.getRegion(Regions.US_EAST_2));
        InputStream resultS3InputStream = new S3DirectoryGzipConcatInputStream(
                s3Client,
                "s3://" + resultS3Directory.getBucket() + "/" + resultS3Directory.getKey());

        String gcpCredentialFilePath =
                "gbsc-gcp-project-annohive-dev-4f980c934a52-defaultserviceaccount.json";
        GCSClient gcsClient = new GCSClient(gcpCredentialFilePath);
        String gcsDestinationUrl = "gs://krferrit-genome-queries-us-central1/athena-imports";
        String s3DirectoryLastTerm = getLastNonEmptySegmentOfPath(resultS3Directory.getKey());//s3DirectoryTerms[s3DirectoryTerms.length-1];
        String athenaExecutionIdAlphaNum = s3DirectoryLastTerm.replaceAll("-", "");
        gcsDestinationUrl += s3DirectoryLastTerm + ".csv";
        GCSUploadStream gcsUploadStream = new GCSUploadStream(gcsClient, gcsDestinationUrl);

        System.out.println("Transferring to gcs: " + gcsDestinationUrl);
        resultS3InputStream.transferTo(gcsUploadStream);
        gcsUploadStream.close();
        resultS3InputStream.close();

        BigQueryClient bigQueryClient = new BigQueryClient("swarm", gcpCredentialFilePath);
        String bqTableName = "athena_import_" + athenaExecutionIdAlphaNum;
        Table bigQueryTable = bigQueryClient.createVariantTableFromGcs(
                bqTableName,
                gcsDestinationUrl);



    }
}
