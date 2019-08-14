package app.api;

import app.AppLogging;
import app.dao.client.*;
import app.dao.query.CountQuery;
import app.dao.query.VariantQuery;
//import com.amazonaws.services.athena.model.ResultSet;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.BlobId;
import com.google.gson.stream.JsonWriter;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.http.MediaType;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.json.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.ValidationException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.ResultSet;

import static app.dao.client.StringUtils.*;


@RestController
@EnableAutoConfiguration
public class Controller {
    private static final Logger log = AppLogging.getLogger(Controller.class);
    private String gcpCredentialFilePath = "gcp.json";
    private String awsCredentialFilePath = "aws.properties";
    /**
     * Database clients
     */
    private BigQueryClient bigQueryClient;
    private AthenaClient athenaClient;

    /**
     * Storage clients
     */
    private GCSClient gcsClient;
    private S3Client s3Client;
    private Region awsRegion = Region.getRegion(Regions.US_EAST_2);

    public Controller() {
        getBigQueryClient();
        getAthenaClient();
        getGcsClient();
        getS3Client();
    }

    private BigQueryClient getBigQueryClient() {
        if (bigQueryClient == null)
            bigQueryClient = new BigQueryClient("swarm", gcpCredentialFilePath);
        return bigQueryClient;
    }

    private AthenaClient getAthenaClient() {
        if (athenaClient == null)
            athenaClient = new AthenaClient("swarm", awsCredentialFilePath);
        return athenaClient;
    }

    private GCSClient getGcsClient() {
        if (gcsClient == null) gcsClient = new GCSClient(gcpCredentialFilePath);
        return gcsClient;
    }

    private S3Client getS3Client() {
        if (s3Client == null) s3Client = new S3Client(awsCredentialFilePath, awsRegion);
        return s3Client;
    }


    @RequestMapping(
            value = "/variants",
            method = {RequestMethod.GET}
    )
    public void getVariants(
            //@RequestParam(required = false, name = "cloud", defaultValue = "all") String cloudParam,
            @RequestParam(required = false, name = "reference_name") String referenceNameParam,
            @RequestParam(required = false, name = "start_position") String startPositionParam,
            @RequestParam(required = false, name = "end_position") String endPositionParam,
            @RequestParam(required = false, name = "reference_bases") String referenceBasesParam,
            @RequestParam(required = false, name = "alternate_bases") String alternateBasesParam,
            HttpServletRequest request, HttpServletResponse response
    ) throws IOException {
        VariantQuery variantQuery = new VariantQuery();
        try {
            /*if (!StringUtils.isEmpty(cloudParam)) {
                validateCloudParam(cloudParam);
            }*/
            if (!StringUtils.isEmpty(referenceNameParam)) {
                validateReferenceName(referenceNameParam);
                variantQuery.setReferenceName(referenceNameParam);
            }
            if (!StringUtils.isEmpty(startPositionParam)) {
                Long startPosition = validateLongString(startPositionParam);
                variantQuery.setStartPosition(startPosition);
            }
            if (!StringUtils.isEmpty(endPositionParam)) {
                Long endPosition = validateLongString(endPositionParam);
                variantQuery.setEndPosition(endPosition);
            }
            if (!StringUtils.isEmpty(referenceBasesParam)) {
                validateBasesString(referenceBasesParam);
                variantQuery.setReferenceBases(referenceBasesParam);
            }
            if (!StringUtils.isEmpty(alternateBasesParam)) {
                validateBasesString(alternateBasesParam);
                variantQuery.setAlternateBases(alternateBasesParam);
            }
            /*if (!StringUtils.isEmpty(positionRangeParam)) {
                Boolean positionRange = validateBooleanParam(positionRangeParam);
                countQuery.setUsePositionAsRange(positionRange);
            }*/

        } catch (ValidationException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
            return;
        }

        variantQuery.setTableIdentifier("variants");

        String nonce = randomAlphaNumStringOfLength(18);
        String athenaDestinationDataset = "swarm";
        String athenaDestinationTable = "genome_query_" + nonce;
        boolean athenaDeleteResultTable = false;
        String bigqueryDestinationDataset = athenaDestinationDataset;
        String bigqueryDestinationTable = athenaDestinationTable;
        boolean bigqueryDeleteResultTable = false;

        Callable<S3ObjectId> athenaCallable = new Callable<S3ObjectId>() {
            @Override
            public S3ObjectId call() throws Exception {
                S3ObjectId objectId = getAthenaClient().executeVariantQuery(
                        variantQuery,
                        Optional.of(athenaDestinationDataset),
                        Optional.of(athenaDestinationTable),
                        Optional.of(athenaDeleteResultTable));
                log.info("Finished athena executeVariantQuery");
                return objectId;
            }
        };
        Callable<BlobId> bigqueryCallable = new Callable<BlobId>() {
            @Override
            public BlobId call() throws Exception {
                BlobId blobId = getBigQueryClient().executeVariantQuery(
                        variantQuery,
                        Optional.of(bigqueryDestinationDataset),
                        Optional.of(bigqueryDestinationTable),
                        Optional.of(bigqueryDeleteResultTable));
                log.info("Finished bigquery executeVariantQuery");
                return blobId;
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        // TODO
        log.info("Submitting athena query");
        Future<S3ObjectId> athenaFuture = executorService.submit(athenaCallable);
        log.info("Submitting bigquery query");
        Future<BlobId> bigqueryFuture = executorService.submit(bigqueryCallable);
        S3ObjectId athenaResultLocation = null;
        BlobId bigqueryResultLocation = null;

        log.debug("Shutting down executor service");
        executorService.shutdown();
        try {
            long execTimeoutSeconds = 60 * 3;
            log.info("Waiting " + execTimeoutSeconds + " seconds for query threads to complete");
            executorService.awaitTermination(execTimeoutSeconds, TimeUnit.SECONDS);
            log.info("Successfully shut down executor service");
        } catch (InterruptedException e) {
            response.sendError(
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Query executor was interrupted unexpectedly");
            return;
        }

        log.debug("Getting query result locations");
        try {
            long getTimeoutSeconds = 60 * 1;
            athenaResultLocation = athenaFuture.get(getTimeoutSeconds, TimeUnit.SECONDS);
            log.info("Got athena result location");
            bigqueryResultLocation = bigqueryFuture.get(getTimeoutSeconds, TimeUnit.SECONDS);
            log.info("Got bigquery result location");
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            e.printStackTrace();
            response.sendError(
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Failed to retrieve query result location");
            return;
        }

        String athenaResultDirectoryUrl = s3Client.s3ObjectIdToString(athenaResultLocation);
        String bigqueryResultDirectoryUrl = gcsClient.blobIdToString(bigqueryResultLocation);

        // determine sizes of each
        log.info("Getting directory sizes");
        long athenaResultSize = s3Client.getDirectorySize(athenaResultDirectoryUrl);
        long bigqueryResultSize = gcsClient.getDirectorySize(bigqueryResultDirectoryUrl);
        log.info("athena result size: " + athenaResultSize);
        log.info("bigquery result size: " + bigqueryResultSize);

        if (athenaResultSize < bigqueryResultSize) {
            log.info("Performing rest of computation in BigQuery");
            String athenaOutputId = getLastNonEmptySegmentOfPath(athenaResultDirectoryUrl);
            String gcsAthenaImportDirectory = pathJoin(
                    bigQueryClient.getStorageBucket(),
                    "athena-imports/" + athenaOutputId);
            String gcsAthenaImportFile = pathJoin(gcsAthenaImportDirectory, "import.csv");
            log.info("Copying athena output id: " + athenaOutputId
                    + " from " + athenaResultDirectoryUrl
                    + " to " + gcsAthenaImportFile);
            GCSUploadStream gcsUploadStream = new GCSUploadStream(gcsClient, gcsAthenaImportFile);
            S3DirectoryGzipConcatInputStream s3DirGzipInputStream =
                    new S3DirectoryGzipConcatInputStream(s3Client, athenaResultDirectoryUrl);
            log.info("Initiating transfer");
            s3DirGzipInputStream.transferTo(gcsUploadStream);
            log.debug("CLosing S3 input stream");
            s3DirGzipInputStream.close();
            log.debug("Closing GCS output stream");
            gcsUploadStream.close();
            log.debug("Finished transfer from S3 to GCS");
            // create the table from the file uploaded
            String importedAthenaTableName = "athena_import_" + athenaOutputId;
            log.info("Creating table " + importedAthenaTableName + " from directory " + gcsAthenaImportDirectory);
            Table importedAthenaTable = bigQueryClient.createVariantTableFromGcs(
                    importedAthenaTableName, gcsAthenaImportDirectory);
            log.info("Finished creating table: " + importedAthenaTableName);
        } else {
            log.info("Performing rest of computation in Athena");
            // TODO
            String bigqueryOutputId = getLastNonEmptySegmentOfPath(bigqueryResultDirectoryUrl);
            String s3BigQueryImportDirectory = pathJoin(
                    athenaClient.getStorageBucket(),
                    "bigquery-imports/" + bigqueryOutputId + "/");
            String s3BigQueryImportFile = pathJoin(s3BigQueryImportDirectory, "import.csv");
            log.info("Copying bigquery output id: " + bigqueryOutputId
                    + " from " + bigqueryResultDirectoryUrl
                    + " to " + s3BigQueryImportFile);
            S3UploadStream s3UploadStream = new S3UploadStream(s3Client, s3BigQueryImportFile);
            GCSDirectoryGzipConcatInputStream gcsDirGzipInputStream =
                    new GCSDirectoryGzipConcatInputStream(gcsClient, bigqueryResultDirectoryUrl);
            log.info("Initiating transfer");
            gcsDirGzipInputStream.transferTo(s3UploadStream);
            log.debug("Closing GCS input stream");
            gcsDirGzipInputStream.close();
            log.debug("Closing S3 output stream");
            s3UploadStream.close();
            log.debug("Finished transfer from GCS to S3");
            // create the table from the file uploaded
            String importedBigqueryTableName = "bigquery_import_" + bigqueryOutputId;
            log.info("Creating table " + importedBigqueryTableName + " from file " + s3BigQueryImportDirectory);
            athenaClient.createVariantTableFromS3(importedBigqueryTableName, s3BigQueryImportDirectory);
            log.info("Finished creating table: " + importedBigqueryTableName);
        }
        response.setStatus(200);
        JsonWriter jsonWriter = new JsonWriter(response.getWriter());
        jsonWriter.beginObject();
        jsonWriter.name("message").value("hello world");
        jsonWriter.endObject();

    }


    @RequestMapping("/hello")
    public String hello(@RequestParam(value="name") String name) {
        return String.format("Hello %s!", name);
    }

    @RequestMapping(
            value = "/variants/by_gene/{gene_label}",
            produces = "application/json",
            method = RequestMethod.GET)
    //@ResponseBody
    public void variantsByGene(
            @RequestParam(required = false, name = "cloud", defaultValue = "all") String cloudParam,
            @PathVariable("gene_label") String geneLabel,
            HttpServletRequest request, HttpServletResponse response
    ) throws IOException, SQLException {
        try {
            if (!StringUtils.isEmpty(cloudParam)) {
                validateCloudParam(cloudParam);
            }
            log.info("cloudParam = " + cloudParam);
        } catch (ValidationException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
            return;
        }
        Pattern geneNamePattern = Pattern.compile("[a-zA-Z0-9]+");
        Matcher geneNameMatcher = geneNamePattern.matcher(geneLabel);
        if (!geneNameMatcher.matches()) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Gene name did not match regex filter");
            return;
        }


        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Callable<TableResult> gcpCallable = new Callable<TableResult>() {
            @Override
            public TableResult call() throws Exception {
                // get gene coordinates
                String coordSql =
                        "select chromosome, start_position, end_position"
                        + " from swarm.relevant_genes_view_hg19"
                        + " where gene_name = ?";
                QueryJobConfiguration coordJobConfig = QueryJobConfiguration.newBuilder(coordSql)
                        .addPositionalParameter(QueryParameterValue.string(geneLabel))
                        .build();
                TableResult coordTr = getBigQueryClient().runQueryJob(coordJobConfig);
                assert(coordTr.getTotalRows() == 1);
                String chromosome = null;
                Long startPosition = null, endPosition = null;
                FieldValueList fieldValues = coordTr.iterateAll().iterator().next();
                chromosome = fieldValues.get("chromosome").getStringValue();
                startPosition = fieldValues.get("start_position").getLongValue();
                endPosition = fieldValues.get("end_position").getLongValue();

                System.out.printf("Gene %s has hg19 coordinates %s:%d-%d\n",
                        geneLabel, chromosome, startPosition, endPosition);

                // Run query for variants within intersection table overlapping with gene
                String matchSql =
                        "select reference_name, start_position, end_position, reference_bases, alternate_bases, minor_af"
                        + " from swarm.1000genomes_vcf_half1 "
                        + " where reference_name = ? "
                        + " and "
                        + " ((start_position >= ? and start_position <= ?)" // start pos overlaps gene
                        + " or (end_position >= ? and end_position <= ?)" // end pos overlaps gene
                        + " or (start_position < ? and end_position > ?))"; // variant is larger than gene

                QueryJobConfiguration matchJobConfig = QueryJobConfiguration.newBuilder(matchSql)
                        .addPositionalParameter(QueryParameterValue.string(chromosome))
                        .addPositionalParameter(QueryParameterValue.int64(startPosition))
                        .addPositionalParameter(QueryParameterValue.int64(endPosition))
                        .addPositionalParameter(QueryParameterValue.int64(startPosition))
                        .addPositionalParameter(QueryParameterValue.int64(endPosition))
                        .addPositionalParameter(QueryParameterValue.int64(startPosition))
                        .addPositionalParameter(QueryParameterValue.int64(endPosition))
                        .build();
                TableResult tr = getBigQueryClient().runQueryJob(matchJobConfig);
                log.info("finished gcp variant query");
                return tr;
            }
        };
        Callable<ResultSet> awsCallable = new Callable<ResultSet>() {
            @Override
            public ResultSet call() throws Exception {
                log.debug("awsCallable is being called");

                String coordSql =
                        "select chromosome, start_position, end_position"
                        + " from swarm.relevant_genes_view_hg19"
                        + " where gene_name = '" + geneLabel + "'";

                log.info("converting query to prepared statement: [" + coordSql + "]");
                PreparedStatement ps = getAthenaClient().queryToPreparedStatement(coordSql);
                //log.info("setting gene_name parameter in prepared statement");
                //ps.setString(1, geneLabel);
                log.info("running gene lookup query");
                ResultSet rs = ps.executeQuery();
                rs.next();
                String chromosome = rs.getString("chromosome");
                Long startPosition = rs.getLong("start_position");
                Long endPosition = rs.getLong("end_position");

                log.info(String.format("Gene %s has hg19 coordinates %s:%d-%d\n",
                        geneLabel, chromosome, startPosition, endPosition));

                // Run query for variants overlapping with gene
                String matchSql =
                        "select reference_name, start_position, end_position, reference_bases, alternate_bases, minor_af"
                        + " from swarm.thousandgenomes_vcf_half2"
                        + " where reference_name = ?"
                        + " and"
                        + " ((start_position >= ? and start_position <= ?)" // start pos overlaps gene
                        + " or (end_position >= ? and end_position <= ?)" // end pos overlaps gene
                        + " or (start_position < ? and end_position > ?))";
                matchSql = matchSql.replaceFirst("\\?", String.format("'%s'", chromosome));
                matchSql = matchSql.replaceFirst("\\?", String.valueOf(startPosition));
                matchSql = matchSql.replaceFirst("\\?", String.valueOf(endPosition));
                matchSql = matchSql.replaceFirst("\\?", String.valueOf(startPosition));
                matchSql = matchSql.replaceFirst("\\?", String.valueOf(endPosition));
                matchSql = matchSql.replaceFirst("\\?", String.valueOf(startPosition));
                matchSql = matchSql.replaceFirst("\\?", String.valueOf(endPosition));
                log.info("running variant lookup query for gene " + geneLabel + ",: " + matchSql);
                PreparedStatement ps2 = getAthenaClient().queryToPreparedStatement(matchSql);

                ResultSet awsResult = ps2.executeQuery();
                log.info("finished aws variant query");
                return awsResult;
            }
        };

        Future<TableResult> gcpFuture = null;
        if (cloudParam.equals("gcp") || cloudParam.equals("all")) {
            gcpFuture = (executorService.submit(gcpCallable));
        }
        Future<ResultSet> awsFuture = null;
        if (cloudParam.equals("aws") || cloudParam.equals("all")) {
            awsFuture = executorService.submit(awsCallable);
        }

        // synchronous wait for callables in executorservice to finish
        executorService.shutdown();
        try {
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Query was interrupted unexpectedly");
            return;
        }

        TableResult gcpTableResult = null;
        ResultSet awsResultSet = null;
        try {
            gcpTableResult = gcpFuture != null ? gcpFuture.get() : null;
            awsResultSet = awsFuture != null ? awsFuture.get() : null;
        } catch (ExecutionException | InterruptedException e) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }

        long startTime = System.currentTimeMillis();

        // write headers
        response.setHeader("Content-Type", MediaType.APPLICATION_JSON_UTF8_VALUE);

        // write data
        JsonWriter jsonWriter = new JsonWriter(response.getWriter());
        jsonWriter.beginObject();
        jsonWriter.name("results");
        jsonWriter.beginObject();

        if (gcpTableResult != null) {
            jsonWriter.name("gcp");
            jsonWriter.beginObject();
            jsonWriter.name("count").value(gcpTableResult.getTotalRows());
            jsonWriter.name("results");
            jsonWriter.beginArray();
            Iterator<FieldValueList> iterator = gcpTableResult.iterateAll().iterator();
            while (iterator.hasNext()) {
                FieldValueList fvl = iterator.next();
                jsonWriter.beginObject();
                jsonWriter.name("reference_name").value(fvl.get("reference_name").getStringValue());
                jsonWriter.name("start_position").value(fvl.get("start_position").getLongValue());
                jsonWriter.name("end_position").value(fvl.get("end_position").getLongValue());
                jsonWriter.name("reference_bases").value(fvl.get("reference_bases").getStringValue());
                jsonWriter.name("alternate_bases").value(fvl.get("alternate_bases").getStringValue());
                jsonWriter.name("minor_af").value(fvl.get("minor_af").getDoubleValue());
                jsonWriter.endObject();
            }
            jsonWriter.endArray(); // end gcp results array
            jsonWriter.endObject(); // end gcp object
        }

        if (awsResultSet != null) {
            log.debug("adding aws results object");
            jsonWriter.name("aws");
            jsonWriter.beginObject();
            jsonWriter.name("results");
            jsonWriter.beginArray();
            long awsResultCount = 0;

            while (awsResultSet.next()) {
                jsonWriter.beginObject();
                int i = 1;
                jsonWriter.name("reference_name").value(awsResultSet.getString(i++));
                jsonWriter.name("start_position").value(awsResultSet.getLong(i++));
                jsonWriter.name("end_position").value(awsResultSet.getLong(i++));
                jsonWriter.name("reference_bases").value(awsResultSet.getString(i++));
                jsonWriter.name("alternate_bases").value(awsResultSet.getString(i++));
                jsonWriter.name("minor_af").value(Double.valueOf(awsResultSet.getString(i++)));
                /*jsonWriter.name("reference_name").value(data.get(i++).getVarCharValue());
                jsonWriter.name("start_position").value(data.get(i++).getVarCharValue());
                jsonWriter.name("end_position").value(data.get(i++).getVarCharValue());
                jsonWriter.name("reference_bases").value(data.get(i++).getVarCharValue());
                jsonWriter.name("alternate_bases").value(data.get(i++).getVarCharValue());
                jsonWriter.name("minor_af").value(data.get(i++).getVarCharValue());*/
                jsonWriter.endObject();
                awsResultCount++;
            }

            jsonWriter.endArray();
            jsonWriter.name("count").value(awsResultCount);
            jsonWriter.endObject(); // end aws object
        }

        jsonWriter.endObject(); // end results object
        jsonWriter.endObject(); // end top level container
        long endTime = System.currentTimeMillis();
        System.out.printf("Writing response took %dms\n", (endTime - startTime));
    }

    @RequestMapping(value = "/variants/allele_count", produces = "application/json", method = RequestMethod.GET)
    @ResponseBody
    public String alleleCount(
            @RequestParam(required = false, name = "cloud", defaultValue = "all") String cloudParam,
            @RequestParam(required = false, name = "reference_name") String referenceNameParam,
            @RequestParam(required = false, name = "start_position") String startPositionParam,
            @RequestParam(required = false, name = "end_position") String endPositionParam,
            @RequestParam(required = false, name = "reference_bases") String referenceBasesParam,
            @RequestParam(required = false, name = "alternate_bases") String alternateBasesParam,
            // set to true to check any in [start, end] range instead of exact matches
            //@RequestParam(required = false, name = "position_range", defaultValue = "false") String positionRangeParam,
            HttpServletRequest request, HttpServletResponse response
    ) throws IOException {
        CountQuery countQuery = new CountQuery();

        try {
            if (!StringUtils.isEmpty(cloudParam)) {
                validateCloudParam(cloudParam);
            }
            if (!StringUtils.isEmpty(referenceNameParam)) {
                validateReferenceName(referenceNameParam);
                countQuery.setReferenceName(referenceNameParam);
            }
            if (!StringUtils.isEmpty(startPositionParam)) {
                Long startPosition = validateLongString(startPositionParam);
                countQuery.setStartPosition(startPosition);
            }
            if (!StringUtils.isEmpty(endPositionParam)) {
                Long endPosition = validateLongString(endPositionParam);
                countQuery.setEndPosition(endPosition);
            }
            if (!StringUtils.isEmpty(referenceBasesParam)) {
                validateBasesString(referenceBasesParam);
                countQuery.setReferenceBases(referenceBasesParam);
            }
            if (!StringUtils.isEmpty(alternateBasesParam)) {
                validateBasesString(alternateBasesParam);
                countQuery.setAlternateBases(alternateBasesParam);
            }
            /*if (!StringUtils.isEmpty(positionRangeParam)) {
                Boolean positionRange = validateBooleanParam(positionRangeParam);
                countQuery.setUsePositionAsRange(positionRange);
            }*/

        } catch (ValidationException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
            return null;
        }

        // return count json
        JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Callable<Long> gcpCallable = new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                long count = getBigQueryClient().executeCount(countQuery, "thousandgenomes_vcf_half1");
                jsonBuilder.add("count", count);
                return count;
            }
        };
        Callable<Long> awsCallable = new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                //long count = getBigQueryClient().executeCount(countQuery, "thousandgenomes_vcf_half1");
                //jsonBuilder.add("count", count);
                //return count;
                throw new UnsupportedOperationException("AWS Client is not fully implemented");
            }
        };

        List<Future<Long>> futures = new ArrayList<>();
        if (cloudParam.equals("gcp") || cloudParam.equals("all")) {
            futures.add(executorService.submit(gcpCallable));
        }
        if (cloudParam.equals("aws") || cloudParam.equals("all")) {
            futures.add(executorService.submit(awsCallable));
        }


        executorService.shutdown();
        try {
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Query was interrupted unexpectedly");
            return null;
        }

        for (Future<Long> future : futures) {
            try {
                future.get();
            } catch (ExecutionException | InterruptedException e) {
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
            }
        }

        return jsonBuilder.build().toString();
    }


    @RequestMapping(value = "/count", produces = "application/json", method = RequestMethod.GET)
    @ResponseBody
    public String count(
            @RequestParam(required = false, name = "cloud", defaultValue = "all") String cloudParam,
            @RequestParam(required = false, name = "reference_name") String referenceNameParam,
            @RequestParam(required = false, name = "start_position") String startPositionParam,
            @RequestParam(required = false, name = "end_position") String endPositionParam,
            @RequestParam(required = false, name = "reference_bases") String referenceBasesParam,
            @RequestParam(required = false, name = "alternate_bases") String alternateBasesParam,
            // set to true to check any in [start, end] range instead of exact matches
            @RequestParam(required = false, name = "position_range", defaultValue = "false") String positionRangeParam,
            HttpServletRequest request, HttpServletResponse response
    ) throws IOException {
        CountQuery countQuery = new CountQuery();

        try {
            if (!StringUtils.isEmpty(cloudParam)) {
                validateCloudParam(cloudParam);
            }
            if (!StringUtils.isEmpty(referenceNameParam)) {
                validateReferenceName(referenceNameParam);
                countQuery.setReferenceName(referenceNameParam);
            }
            if (!StringUtils.isEmpty(startPositionParam)) {
                Long startPosition = validateLongString(startPositionParam);
                countQuery.setStartPosition(startPosition);
            }
            if (!StringUtils.isEmpty(endPositionParam)) {
                Long endPosition = validateLongString(endPositionParam);
                countQuery.setEndPosition(endPosition);
            }
            if (!StringUtils.isEmpty(referenceBasesParam)) {
                validateBasesString(referenceBasesParam);
                countQuery.setReferenceBases(referenceBasesParam);
            }
            if (!StringUtils.isEmpty(alternateBasesParam)) {
                validateBasesString(alternateBasesParam);
                countQuery.setAlternateBases(alternateBasesParam);
            }
            if (!StringUtils.isEmpty(positionRangeParam)) {
                Boolean positionRange = validateBooleanParam(positionRangeParam);
                countQuery.setUsePositionAsRange(positionRange);
            }

        } catch (ValidationException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
            return null;
        }

        // return count json
        JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Callable<Long> gcpCallable = new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                long count = getBigQueryClient().executeCount(countQuery, "thousandgenomes_vcf_half1");
                jsonBuilder.add("count", count);
                return count;
            }
        };
        Callable<Long> awsCallable = new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                //long count = getBigQueryClient().executeCount(countQuery, "thousandgenomes_vcf_half1");
                //jsonBuilder.add("count", count);
                //return count;
                throw new UnsupportedOperationException("AWS Client is not fully implemented");
            }
        };

        List<Future<Long>> futures = new ArrayList<>();
        if (cloudParam.equals("gcp") || cloudParam.equals("all")) {
            futures.add(executorService.submit(gcpCallable));
        }
        if (cloudParam.equals("aws") || cloudParam.equals("all")) {
            futures.add(executorService.submit(awsCallable));
        }


        executorService.shutdown();
        try {
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Query was interrupted unexpectedly");
            return null;
        }

        for (Future<Long> future : futures) {
            try {
                future.get();
            } catch (ExecutionException | InterruptedException e) {
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
            }
        }

        return jsonBuilder.build().toString();
    }


    private Boolean validateBooleanParam(String param) {
        if (param.equalsIgnoreCase("true")) {
            return true;
        } else if (param.equalsIgnoreCase("false")) {
            return false;
        } else {
            throw new ValidationException("Invalid boolean value");
        }
    }

    private void validateCloudParam(String cloud) {
        if (StringUtils.isEmpty(cloud)) {
            return;
        }
        ArrayList<String> allowedClouds = new ArrayList<String>();
        allowedClouds.add("aws");
        allowedClouds.add("gcp");
        allowedClouds.add("all");

        if (!allowedClouds.contains(cloud)) {
            throw new ValidationException("Cloud param " + cloud + " not allowed");
        }
    }


    private void validateReferenceName(String referenceName) {
        if (StringUtils.isEmpty(referenceName)) {
            return;
        }
        if (StringUtils.containsWhitespace(referenceName)) {
            throw new ValidationException("Cannot contain whitespace");
        }
        ArrayList<String> referenceNames = new ArrayList<>();
        for (int i = 1; i <= 22; i++) {
            referenceNames.add(Integer.toString(i));
        }
        referenceNames.add("X");
        referenceNames.add("Y");

        if (!referenceNames.contains(referenceName)) {
            throw new ValidationException("Unknown reference name");
        }
    }

    private void validateBasesString(String basesString) {
        if (StringUtils.isEmpty(basesString)) {
            return;
        }
        if (StringUtils.containsWhitespace(basesString)) {
            throw new ValidationException("Cannot contain whitespace");
        }
        Pattern p = Pattern.compile("[ACTG]*");
        Matcher m = p.matcher(basesString);

        if (!m.matches()) {
            throw new ValidationException("Bases must be a sequence of A, C, T, or G");
        }
    }

    private Long validateLongString(String longStr) {
        if (StringUtils.containsWhitespace(longStr)) {
            throw new ValidationException("Cannot contain whitespace");
        }
        try {
            return Long.parseLong(longStr);
        } catch (NumberFormatException e) {
            throw new ValidationException("Parameter must be a 64 bit integer");
        }
    }
}
