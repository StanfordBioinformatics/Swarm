package app.api;

import app.AppLogging;
import app.dao.client.*;
import app.dao.query.VariantQuery;
import app.dao.client.StringUtils;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.athena.model.*;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.BlobId;
import com.google.gson.stream.JsonWriter;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
//import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.ValidationException;
import javax.validation.constraints.NotNull;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
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
    //private Region awsRegion = Region.getRegion(Regions.US_EAST_2);
    private Region awsRegion = Region.getRegion(Regions.US_WEST_1);


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

    private class GeneCoordinate {
        public String referenceName;
        public Long startPosition;
        public Long endPosition;
    }

    public class TableDatabasePair {
        private String tableName;
        private String databaseName;

        public TableDatabasePair(String tableName, String databaseName) {
            this.tableName = tableName;
            this.databaseName = databaseName;
        }

        public String getTableName() {return this.tableName;}

        public String getDatabaseName() {return this.databaseName;}
    }

    public String loadSqlFile(String basename) throws IOException {
        final String MAIN_ROOT = System.getProperty("user.dir") + "/src/main/";
        String path = MAIN_ROOT + "sql/" + basename;
        byte[] bytes = Files.readAllBytes(Paths.get(path));
        log.info("Loading sql file: " + path);
        String sql = new String(bytes, StandardCharsets.US_ASCII);
        return sql;
    }

    @RequestMapping(value = "/stat_by_gene/{gene_label}", method = {RequestMethod.GET})
    public void getStatByGene(
            @PathVariable("gene_label") String geneLabel,
            HttpServletRequest request, HttpServletResponse response
    ) throws InterruptedException, ExecutionException, TimeoutException, IOException {
        GeneCoordinate geneCoordinate = getGeneCoordinates(geneLabel);
        String referenceName = geneCoordinate.referenceName;
        String startPosition = geneCoordinate.startPosition.toString();
        String endPosition = geneCoordinate.endPosition.toString();
        String referenceBases = null;
        String alternateBases = null;
        String positionRange = "true";
        log.info("Delegating stat_by_gene to getStat");
        this.getStat(
                referenceName,
                startPosition, endPosition,
                referenceBases, alternateBases,
                positionRange,
                request, response
        );
    }

    /**
     * Use Case 1, Stat query. Checks for existence of variants.
     *
     */
    @RequestMapping(value = "/stat", method = {RequestMethod.GET})
    public void getStat(
            @RequestParam(required = false, name = "reference_name") String referenceNameParam,
            @RequestParam(required = false, name = "start_position") String startPositionParam,
            @RequestParam(required = false, name = "end_position") String endPositionParam,
            @RequestParam(required = false, name = "reference_bases") String referenceBasesParam,
            @RequestParam(required = false, name = "alternate_bases") String alternateBasesParam,
            @RequestParam(required = false, name = "position_range", defaultValue = "true") String positionRange,
            HttpServletRequest request, HttpServletResponse response
    ) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        doCountOrStat(
                referenceNameParam,
                startPositionParam,
                endPositionParam,
                referenceBasesParam,
                alternateBasesParam,
                positionRange,
                Optional.of(true), // statOnly = true
                request,
                response);
    }

    /**
     * Use Case 2, Count query.  Checks for matches
     * @param referenceNameParam
     * @param startPositionParam
     * @param endPositionParam
     * @param referenceBasesParam
     * @param alternateBasesParam
     * @param positionRange
     * @param request
     * @param response
     * @throws IOException
     * @throws InterruptedException
     */
    @RequestMapping(value = "/count", method = {RequestMethod.GET})
    public void getCount(
            @RequestParam(required = false, name = "reference_name") String referenceNameParam,
            @RequestParam(required = false, name = "start_position") String startPositionParam,
            @RequestParam(required = false, name = "end_position") String endPositionParam,
            @RequestParam(required = false, name = "reference_bases") String referenceBasesParam,
            @RequestParam(required = false, name = "alternate_bases") String alternateBasesParam,
            @RequestParam(required = false, name = "position_range", defaultValue = "true") String positionRange,
            //@Nullable Optional<Boolean> statOnly,
            HttpServletRequest request, HttpServletResponse response
    ) throws IOException, InterruptedException, TimeoutException, ExecutionException {
        doCountOrStat(
                referenceNameParam,
                startPositionParam,
                endPositionParam,
                referenceBasesParam,
                alternateBasesParam,
                positionRange,
                Optional.of(false), // statOnly = false
                request,
                response);
    }

    public void doCountOrStat(
            String referenceNameParam,
            String startPositionParam,
            String endPositionParam,
            String referenceBasesParam,
            String alternateBasesParam,
            String positionRange,
            @Nullable Optional<Boolean> statOnly,
            HttpServletRequest request, HttpServletResponse response
    ) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        VariantQuery variantQuery = new VariantQuery();
        try {
            if (!StringUtils.isEmpty(positionRange)) {
                if (positionRange.equalsIgnoreCase("true")) {
                    log.debug("Using positions as a range");
                    variantQuery.setUsePositionAsRange();
                } else if (positionRange.equalsIgnoreCase("false")) {
                    // nothing
                } else {
                    throw new ValidationException("Invalid param for position_range, must be true or false");
                }
            }
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
        } catch (ValidationException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
            return;
        }
        variantQuery.setTableIdentifier("variants");

        // set it to only count the matching records, not return them
        variantQuery.setCountOnly(true);

        Callable<Long> athenaCallable = new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return athenaClient.executeCount(variantQuery);
            }
        };
        Callable<Long> bigqueryCallable = new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return bigQueryClient.executeCount(variantQuery);
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        log.info("Submitting athena query");
        Future<Long> athenaFuture = executorService.submit(athenaCallable);
        log.info("Submitting bigquery query");
        Future<Long> bigqueryFuture = executorService.submit(bigqueryCallable);
        Long athenaCount = null;
        Long bigqueryCount = null;

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

        log.debug("Getting query result counts");
        try {
            long getTimeoutSeconds = 60;
            athenaCount = athenaFuture.get(getTimeoutSeconds, TimeUnit.SECONDS);
            log.info("Got athena count: " + athenaCount);
            bigqueryCount = bigqueryFuture.get(getTimeoutSeconds, TimeUnit.SECONDS);
            log.info("Got bigquery count: " + bigqueryCount);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            //e.printStackTrace();
            response.sendError(
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Failed to retrieve query counts");
            throw e;
        }
        PrintWriter pw = response.getWriter();
        pw.write("Athena,BigQuery\n");
        if (statOnly == null || !statOnly.isPresent()) {
            log.debug("Writing count results");
            pw.write(String.format("%d,%d\n", athenaCount, bigqueryCount));
        } else {
            log.debug("Writing stat results");
            pw.write(String.format("%s,%s\n",
                    athenaCount > 0 ? "true" : "false",
                    bigqueryCount > 0 ? "true" : "false"));
        }

        log.debug("Wrote results to response stream");
    }


    @RequestMapping(
            value = "/variants_by_gene/{gene_label}",
            method = {RequestMethod.GET}
    )
    public void getVariantsByGene(
            @PathVariable("gene_label") String geneLabel,
            @RequestParam(required = false, defaultValue = "all", name = "sourceCloud") String sourceCloud,
            @RequestParam(required = false, name = "return_results", defaultValue = "false") String returnResultsParam,
            HttpServletRequest request, HttpServletResponse response
    ) throws IOException, SQLException, InterruptedException, TimeoutException, ExecutionException {
        boolean returnResults = false;
        if (!StringUtils.isEmpty(returnResultsParam)) {
            if (returnResultsParam.equals("true")) {
                returnResults = true;
            } else if (!returnResultsParam.equals("false")) {
                throw new ValidationException("Invalid param for return_results, must be true or false");
            }
        }

        GeneCoordinate geneCoordinate = getGeneCoordinates(geneLabel);
        if (geneCoordinate == null) {
            throw new IllegalArgumentException("Gene could not be found in coordinates table");
        }
        log.info("Delegating to Controller.getVariants");

        SwarmTableIdentifier swarmTableIdentifier = this.getVariants(
                sourceCloud,
                geneCoordinate.referenceName,
                geneCoordinate.startPosition.toString(),
                geneCoordinate.endPosition.toString(),
                null,
                null,
                "true");

        if (swarmTableIdentifier == null) {
            throw new IllegalArgumentException("Failed to query for variants");
        }

        response.setHeader("Content-Type", "application/json");
        JsonWriter jsonWriter = new JsonWriter(response.getWriter());
        jsonWriter.beginObject();

        if (swarmTableIdentifier.databaseType.equals("athena")) {
            athenaClient.serializeTableToJSON(swarmTableIdentifier.tableName, jsonWriter, returnResults);
        } else if (swarmTableIdentifier.databaseType.equals("bigquery")) {
            bigQueryClient.serializeTableToJson(swarmTableIdentifier.tableName, jsonWriter, returnResults);
        } else {
            throw new IllegalStateException("Unknown databaseType " + swarmTableIdentifier.databaseType);
        }

        if (!returnResults) {
            jsonWriter.name("data_message").value("To return data in response, set return_results query parameter to true");
        }

        jsonWriter.endObject();
//
//        this.getVariants(
//                sourceCloud,
//                geneCoordinate.referenceName,
//                geneCoordinate.startPosition.toString(),
//                geneCoordinate.endPosition.toString(),
//                null,
//                null,
//                "true",
//                "true",
//                request,
//                response);
    }

    /**
     * Returns a map with two entries, half1 and half2. These are in turn a map of
     * SuperPopulation label to a list of sample labels.
     *
     * @param superPopulation - optional SuperPopulation value (ex: EUR, AFR, SAS, EAS)
     * @return superPopulation to sample map in each half table
     * @throws IOException an IO operation failed
     * @throws InterruptedException a call was interrupted
     */
    private Map<String,Map<String,List<String>>> getSuperPopulationSamples(@NotNull Optional<String> superPopulation) throws IOException, InterruptedException {
        String sql = loadSqlFile("superpopulation-samples.sql");
        if (superPopulation.isPresent()) {
            // TODO create parameterized method in bigquery client
            sql += "\nwhere SuperPopulation = '" + superPopulation.get() + "'";
        }
        Map<String,List<String>> resultsHalf1 = new HashMap<>();
        Map<String,List<String>> resultsHalf2 = new HashMap<>();
        TableResult tr = bigQueryClient.runSimpleQuery(sql);
        Iterable<FieldValueList> fvlIterator = tr.iterateAll();
        for (FieldValueList fvl : fvlIterator) {
            String sp = fvl.get("SuperPopulation").getStringValue();
            String half1 = fvl.get("Half1").getStringValue();
            String half2 = fvl.get("Half2").getStringValue();

            List<String> sampleArray1 = null;
            List<String> sampleArray2 = null;

            // Get or create results array
            if (!resultsHalf1.containsKey(sp)) {
                sampleArray1 = new ArrayList<>();
                resultsHalf1.put(sp, sampleArray1);
            } else {
                sampleArray1 = resultsHalf1.get(sp);
            }
            if (!resultsHalf2.containsKey(sp)) {
                sampleArray2 = new ArrayList<>();
                resultsHalf2.put(sp, sampleArray2);
            } else {
                sampleArray2 = resultsHalf2.get(sp);
            }

            // Store the results
            for (String sample : half1.split(",")) {
                sampleArray1.add(sample);
            }
            for (String sample : half2.split(",")) {
                sampleArray2.add(sample);
            }
        }
        Map<String,Map<String,List<String>>> retMap = new HashMap<>();
        retMap.put("half1", resultsHalf1);
        retMap.put("half2", resultsHalf2);
        return retMap;
    }

    private GeneCoordinate getGeneCoordinates(String geneLabel) throws InterruptedException, ExecutionException, TimeoutException {
        Pattern geneNamePattern = Pattern.compile("[a-zA-Z0-9]+");
        Matcher geneNameMatcher = geneNamePattern.matcher(geneLabel);
        if (!geneNameMatcher.matches()) {
            throw new ValidationException("Gene name did not match regex filter");
            //response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Gene name did not match regex filter");
            //return;
        }

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Callable<GeneCoordinate> geneLookupCallable = new Callable<GeneCoordinate>() {
            @Override
            public GeneCoordinate call() throws Exception {
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
                Iterator<FieldValueList> fieldValueListIterator = coordTr.iterateAll().iterator();
                if (!fieldValueListIterator.hasNext()) {
                    log.warn("No coordinate found for gene: " + geneLabel);
                    return null;
                }
                FieldValueList fieldValues = fieldValueListIterator.next();
                GeneCoordinate coordinate = new GeneCoordinate();
                coordinate.referenceName = fieldValues.get("chromosome").getStringValue();
                coordinate.startPosition = fieldValues.get("start_position").getLongValue();
                coordinate.endPosition = fieldValues.get("end_position").getLongValue();

                System.out.printf("Gene %s has hg19 coordinates %s:%d-%d\n",
                        geneLabel, coordinate.referenceName, coordinate.startPosition, coordinate.endPosition);
                return coordinate;
            }
        };

        final GeneCoordinate geneCoordinate;
        Future<GeneCoordinate> coordinateFuture = executorService.submit(geneLookupCallable);
        executorService.shutdown();
        try {
            geneCoordinate = coordinateFuture.get(3 * 60, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            //e.printStackTrace();
            //response.sendError(500, "Failed to determine gene coordinates");
            //return;
            throw e;
        }
        // this could be null if no coordinate is found
        return geneCoordinate;
    }



    @RequestMapping(value = "/count/by_gene/{gene_label}/by_superpopulation", method = {RequestMethod.GET})
    public void getSuperPopulationCounts(
            @RequestParam(required = false, name = "cloud", defaultValue = "all") String sourceCloud,
            //@RequestParam(required = false, name = "reference_name") String referenceNameParam,
            //@RequestParam(required = false, name = "start_position") String startPositionParam,
            //@RequestParam(required = false, name = "end_position") String endPositionParam,
            //@RequestParam(required = false, name = "reference_bases") String referenceBasesParam,
            //@RequestParam(required = false, name = "alternate_bases") String alternateBasesParam,
            //@RequestParam(required = false, name = "position_range", defaultValue = "true") String positionRange,
            @RequestParam(required = false, name = "superpopulation", defaultValue = "") String superPopulationParam,
            @PathVariable("gene_label") String geneLabel,
            HttpServletRequest request, HttpServletResponse response
    ) throws InterruptedException, SQLException, IOException {
        // TODO validation
        List<String> superPopulationFilter = null;
        boolean doSuperPopulationFilter = true;
        if (superPopulationParam.isEmpty()) {
            doSuperPopulationFilter = false;
        } else {
            superPopulationFilter = Arrays.asList(superPopulationParam.split(","));
        }
//        TableDatabasePair tdp = this.getVariants(
//                sourceCloud,
//                referenceNameParam,
//                startPositionParam,
//                endPositionParam,
//                referenceBasesParam,
//                alternateBasesParam,
//                positionRange,
//                "false",
//                request, response);

//        String selectTemplate =
//                "(select\n" +
//                "  reference_name, pos, id, ref, alt,\n" +
//                "  (HG01488_alleleCount + HG00632_alleleCount) as AFR_alleleCount\n" +
//                "from\n" +
//                "  (select reference_name, pos, id, ref, alt,\n" +
//                "{{case_statements}}" +
//                "    from `gbsc-gcp-project-annohive-dev.1000.1000Orig_half1`) as count_conversion\n" +
//                ")";

        String caseTemplate =
                //"{{sample_name}},\n" +
                "  (case {{sample_name}}\n" +
                "    when '0' then 0 when '1' then 1 when '2' then 1 when '3' then 1\n" +
                "    when '0|0' then 0 when '0|1' then 1 when '0|2' then 1 when '0|3' then 1\n" +
                "    when '1|0' then 1 when '1|1' then 2 when '1|2' then 2 when '1|3' then 2\n" +
                "    when '2|0' then 1 when '2|1' then 2 when '2|2' then 2 when '2|3' then 2\n" +
                "    when '3|0' then 1 when '3|1' then 2 when '3|2' then 2 when '3|3' then 2\n" +
                "    else 0 end\n" +
                "  ) as {{sample_name}}_alleleCount";

        Map<String, Map<String, List<String>>> superPopulationMap = getSuperPopulationSamples(Optional.empty());
        ArrayList<String> superPopulationLabels = new ArrayList<>();
        StringBuilder caseStatements = new StringBuilder();

        StringBuilder sumFields = new StringBuilder();

        if (!superPopulationMap.containsKey("half1")) {
            throw new IllegalArgumentException("SuperPopulation table must contain half1");
        }
        if (!superPopulationMap.containsKey("half2")) {
            throw new IllegalArgumentException("SuperPopulation table must contain half2");
        }
        Map<String,List<String>> half1Map = superPopulationMap.get("half1");
        Map<String,List<String>> half2Map = superPopulationMap.get("half2");

        long spCount = 0;
        long caseCount = 0;
        for (Map.Entry<String,List<String>> entry : half1Map.entrySet()) {
            String superPopulation = entry.getKey();
            List<String> sampleList = entry.getValue();

            if (doSuperPopulationFilter && !superPopulationFilter.contains(superPopulation)) {
                log.debug("Skipping super population " + superPopulation + ", not in filter");
                continue;
            }

            if (spCount > 0) {
                sumFields.append(",\n");
            }
            sumFields.append("(");
            for (int i = 0; i < sampleList.size(); i++) {
                String sampleName = sampleList.get(i);
                if (StringUtils.containsWhitespace(sampleName)) {
                    log.error("Sample label " + sampleName + " cannot contain whitespace");
                    throw new IllegalArgumentException("Sample label cannot contain whitespace");
                }

                // Add case statement for this sample
                if (caseCount > 0) {
                    caseStatements.append(",\n");
                }
                caseStatements.append(caseTemplate.replaceAll("\\{\\{sample_name}}", sampleName));
                caseCount++;
                // Add to the sum statement
                if (i > 0) {
                    sumFields.append(" + ");
                }
                //String sampleACField = sampleName + "_alleleCount";
                sumFields.append(sampleName + "_alleleCount");
            }
            sumFields.append(") as ").append(superPopulation).append("_alleleCount");
            spCount++;
        }
        for (Map.Entry<String,List<String>> entry : half2Map.entrySet()) {
            String superPopulation = entry.getKey();
            List<String> sampleList = entry.getValue();

            if (doSuperPopulationFilter && !superPopulationFilter.contains(superPopulation)) {
                log.debug("Skipping super population " + superPopulation + ", not in filter");
                continue;
            }

            if (spCount > 0) {
                sumFields.append(",\n");
            }
            sumFields.append("(");
            for (int i = 0; i < sampleList.size(); i++) {
                String sampleName = sampleList.get(i);
                if (StringUtils.containsWhitespace(sampleName)) {
                    log.error("Sample label " + sampleName + " cannot contain whitespace");
                    throw new IllegalArgumentException("Sample label cannot contain whitespace");
                }

                // Add case statement for this sample
                if (caseCount > 0) {
                    caseStatements.append(",\n");
                }
                caseStatements.append(caseTemplate.replaceAll("\\{\\{sample_name}}", sampleName));
                caseCount++;
                // Add to the sum statement
                if (i > 0) {
                    sumFields.append(" + ");
                }
                //String sampleACField = sampleName + "_alleleCount";
                sumFields.append(sampleName + "_alleleCount");
            }
            sumFields.append(") as ").append(superPopulation).append("_alleleCount");
            spCount++;
        }

        String half1Tablename = "`gbsc-gcp-project-annohive-dev.1000.1000Orig_half1`";

        String sql = "select reference_name, pos, id, ref, alt,\n "; // not including INFO field

        boolean snpOnly = true;

        String sumSelect =
                sumFields.toString() + " \n " +
                "from \n"+
                "(select reference_name, pos, id, ref, alt, \n" +
                "    " + caseStatements.toString() +
                "\nfrom " + half1Tablename;

        if (snpOnly) {
            sumSelect += "\nwhere length(ref) = 1 and length(alt) = 1";
        }
        sumSelect += ")";

        sql += sumSelect;
        //        .replaceAll("\\{\\{sum_fields}}", sumFields.toString())
        //        .replaceAll("\\{\\{count_conversion}}", caseStatements.toString());

        //log.info("Sample Super Population Query:\n" + sql);
        FileWriter queryFileWriter = new FileWriter(new File("query.txt"));
        queryFileWriter.write(sql);
        queryFileWriter.flush();

        // TODO

    }


    /**
     * Use Case 3: computing allele frequency across data sets
     * @param referenceNameParam reference name
     * @param startPositionParam start position
     * @param endPositionParam end position
     * @param referenceBasesParam reference bases
     * @param alternateBasesParam alternate bases
     * @param positionRange use the positions as a range, not an exact match
     * @param request HttpRequest object
     * @param response HttpResponse object
     * @throws IOException an IO operation failed
     * @throws SQLException a sql execution was invalid
     * @throws InterruptedException a call was interrupted
     */
    @RequestMapping(
            value = "/variants",
            method = {RequestMethod.GET}
    )
    private void executeVariants(
            @RequestParam(required = false, name = "cloud", defaultValue = "all") String sourceCloud,
            @RequestParam(required = false, name = "reference_name") String referenceNameParam,
            @RequestParam(required = false, name = "start_position") String startPositionParam,
            @RequestParam(required = false, name = "end_position") String endPositionParam,
            @RequestParam(required = false, name = "reference_bases") String referenceBasesParam,
            @RequestParam(required = false, name = "alternate_bases") String alternateBasesParam,
            @RequestParam(required = false, name = "rsid") String rsidParam,
            @RequestParam(required = false, name = "position_range", defaultValue = "true") String positionRange,
            @RequestParam(required = false, name = "return_results", defaultValue = "false") String returnResultsParam,
            HttpServletRequest request, HttpServletResponse response
    ) throws IOException, SQLException, InterruptedException {
        boolean returnResults = false;
        if (!StringUtils.isEmpty(returnResultsParam)) {
            if (returnResultsParam.equals("true")) {
                returnResults = true;
            } else if (!returnResultsParam.equals("false")) {
                throw new ValidationException("Invalid param for return_results, must be true or false");
            }
        }

        SwarmTableIdentifier swarmTableIdentifier = getVariants(
                sourceCloud,
                referenceNameParam,
                startPositionParam,
                endPositionParam,
                referenceBasesParam,
                alternateBasesParam,
                positionRange,
                rsidParam);

        log.debug("Finished getVariants call");

        if (swarmTableIdentifier == null) {
            throw new RuntimeException("Failed to query variants");
        }

        log.debug("Writing response headers");
        response.setHeader("X-Swarm-DatabaseType", swarmTableIdentifier.databaseType);
        response.setHeader("X-Swarm-DatabaseName", swarmTableIdentifier.databaseName);
        response.setHeader("X-Swarm-TableName", swarmTableIdentifier.tableName);
        response.setHeader("Content-Type", "application/json");

        //StringWriter stringWriter = new StringWriter();
        JsonWriter jsonWriter = new JsonWriter(response.getWriter());
        jsonWriter.beginObject();

        // serialize the table into the response, in JSON format.
        if (swarmTableIdentifier.databaseType.equals("athena")) {
            jsonWriter.name("athena").beginObject();
            athenaClient.serializeTableToJSON(swarmTableIdentifier.tableName, jsonWriter, returnResults);
            jsonWriter.endObject();
        } else if (swarmTableIdentifier.databaseType.equals("bigquery")) {
            jsonWriter.name("bigquery");
            bigQueryClient.serializeTableToJson(swarmTableIdentifier.tableName, jsonWriter, returnResults);
        } else {
            throw new IllegalStateException("Unknown databaseType " + swarmTableIdentifier.databaseType);
        }

        if (!returnResults) {
            jsonWriter.name("data_message").value("To return data in response, set return_results query parameter to true");
        }

        jsonWriter.endObject();
    }

    public static class SwarmTableIdentifier {
        String databaseType; // athena, bigquery, etc
        String databaseName; // example: "swarm"
        String tableName; // within the database, all information needed to reference the table
    }

    private SwarmTableIdentifier getVariants(
            String sourceCloud,
            String referenceNameParam,
            String startPositionParam,
            String endPositionParam,
            String referenceBasesParam,
            String alternateBasesParam,
            String positionRange) throws InterruptedException, SQLException, IOException {
        return getVariants(sourceCloud,
                referenceNameParam,
                startPositionParam,
                endPositionParam,
                referenceBasesParam,
                alternateBasesParam,
                positionRange,
                null);
    }

    private SwarmTableIdentifier getVariants(
            String sourceCloud,
            String referenceNameParam,
            String startPositionParam,
            String endPositionParam,
            String referenceBasesParam,
            String alternateBasesParam,
            String positionRange,
            String rsid
    ) throws IOException, SQLException, InterruptedException {
        VariantQuery variantQuery = new VariantQuery();
        boolean returnResults = false;
        try {
            if (!StringUtils.isEmpty(positionRange)) {
                if (positionRange.equalsIgnoreCase("true")) {
                    log.debug("Using positions as a range");
                    variantQuery.setUsePositionAsRange();
                } else if (positionRange.equalsIgnoreCase("false")) {
                    // nothing
                } else {
                    throw new ValidationException("Invalid param for position_range, must be true or false");
                }
            }
            if (!StringUtils.isEmpty(sourceCloud)) {
                validateCloudParam(sourceCloud);
            }
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
        } catch (ValidationException e) {
            throw e;
            //response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
            //return null;
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

        boolean doBigquery = false, doAthena = false, doAll = false;
        if (sourceCloud.equals("athena") || sourceCloud.equals("all")) {
            doAthena = true;
        }
        if (sourceCloud.equals("bigquery") || sourceCloud.equals("all")) {
            doBigquery = true;
        }
        if (sourceCloud.equals("all") || (doBigquery && doAthena)) {
            doAll = true;
        }

        Future<S3ObjectId> athenaFuture = null;
        if (doAthena) {
            log.info("Submitting athena query");
            athenaFuture = executorService.submit(athenaCallable);
        }
        Future<BlobId> bigqueryFuture = null;
        if (doBigquery) {
            log.info("Submitting bigquery query");
            bigqueryFuture = executorService.submit(bigqueryCallable);
        }

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
            throw e;
//            response.sendError(
//                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
//                    "Query executor was interrupted unexpectedly");
            //return null;
        }

        log.debug("Getting query result locations");
        try {
            long getTimeoutSeconds = 60;
            if (doAthena) {
                athenaResultLocation = athenaFuture.get(getTimeoutSeconds, TimeUnit.SECONDS);
                log.info("Got athena result location");
            }
            if (doBigquery) {
                bigqueryResultLocation = bigqueryFuture.get(getTimeoutSeconds, TimeUnit.SECONDS);
                log.info("Got bigquery result location");
            }
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to retrieve query result location");
//            response.sendError(
//                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
//                    "Failed to retrieve query result location");
            //return null;
        }

        String athenaResultDirectoryUrl = null, bigqueryResultDirectoryUrl = null;
        long athenaResultSize = -1, bigqueryResultSize = -1;

        if (doAthena) {
            athenaResultDirectoryUrl = s3Client.s3ObjectIdToString(athenaResultLocation);
            athenaResultSize = s3Client.getDirectorySize(athenaResultDirectoryUrl);
            log.info("athena result size: " + athenaResultSize);
        }
        if (doBigquery) {
            bigqueryResultDirectoryUrl = gcsClient.blobIdToString(bigqueryResultLocation);
            bigqueryResultSize = gcsClient.getDirectorySize(bigqueryResultDirectoryUrl);
            log.info("bigquery result size: " + bigqueryResultSize);
        }

        boolean resultsInBigQuery = false, resultsInAthena = false;
        String bigqueryResultsTableFullName = null,
                athenaResultsTableFullName = null;

        SwarmTableIdentifier swarmTableIdentifier = new SwarmTableIdentifier();

        /*if (!doAll) {
            //String selectSimpleSql = "select * from %s";
            if (doBigquery) {
                resultsInBigQuery = true;
                bigqueryResultsTableFullName = String.format("`%s.%s.%s`",
                        bigQueryClient.getProjectName(), bigqueryDestinationDataset, bigqueryDestinationTable);
            } else if (doAthena) {
                resultsInAthena = true;
                athenaResultsTableFullName = quoteAthenaTableIdentifier(String.format("%s.%s",
                        athenaDestinationDataset, athenaDestinationTable));
            } else {
                throw new IllegalStateException("Invalid state");
            }
        } else*/ if (athenaResultSize < bigqueryResultSize) {
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
            String tableNameSafeAthenaOutputId = athenaOutputId.replaceAll("-", "");
            log.debug("Converted athena output id from " + athenaOutputId +
                    " to table name safe: " + tableNameSafeAthenaOutputId);
            String importedAthenaTableName = "athena_import_" + tableNameSafeAthenaOutputId;
            log.info("Creating table " + importedAthenaTableName + " from directory " + gcsAthenaImportDirectory);
            Table importedAthenaTable = bigQueryClient.createVariantTableFromGcs(
                    importedAthenaTableName, gcsAthenaImportDirectory);
            log.info("Finished creating table: " + importedAthenaTableName);

            // join the tables together
            String mergeSql = "select\n" +
                    "  a.reference_name, \n" +
                    "  a.start_position,\n" +
                    "  a.end_position,\n" +
                    "  a.reference_bases,\n" +
                    "  a.alternate_bases,\n" +
                    "  (\n" +
                    "    (sum(coalesce(a.allele_count, 0)) + sum(coalesce(b.allele_count, 0))) \n" +
                    "     / \n" +
                    "    (\n" +
                    "      (sum(coalesce(cast(a.minor_af as float64), 0)) \n" +
                    "       + sum(coalesce(cast(b.minor_af as float64), 0))\n" +
                    "       * \n" +
                    "      (sum(coalesce(a.allele_count, 0)) + sum(coalesce(b.allele_count, 0)))" +
                    "    )\n" +
                    "   ) as minor_af,\n" +
                    "  sum(coalesce(a.allele_count, 0)) + sum(coalesce(b.allele_count, 0)) as allele_count\n" +
                    "from \n" +
                    "  swarm.%s b\n" +
                    "full outer join\n" +
                    "  swarm.%s a\n" +
                    "  on a.reference_name = b.reference_name\n" +
                    "  and a.start_position = b.start_position\n" +
                    "  and a.end_position = b.end_position\n" +
                    "  and a.reference_bases = b.reference_bases\n" +
                    "  and a.alternate_bases = b.alternate_bases\n" +
                    "group by a.reference_name, a.start_position, a.end_position, a.reference_bases, a.alternate_bases";
            // first placeholder si bigquery tablename, second is athena tablename
            mergeSql = String.format(mergeSql, bigqueryDestinationTable, importedAthenaTableName);
            log.info(String.format(
                    "Merging tables %s and %s and returning results",
                    bigqueryDestinationTable, importedAthenaTableName));
            String bigqueryMergedTableName = "merge_" + nonce;
            //bigqueryResultsTableFullName = String.format("%s.%s.%s");
            TableId bigqueryMergedTableId = TableId.of(bigqueryDestinationDataset, bigqueryMergedTableName);
            // TODO
            TableResult tr = bigQueryClient.runSimpleQuery(mergeSql, Optional.of(bigqueryMergedTableId));
            //TableResult tr = bigQueryClient.runSimpleQuery(mergeSql);
            log.info("Finished merge query in BigQuery");
            log.info("Writing response headers");
//            response.setStatus(200);
//            response.setHeader("Content-Type", "text/csv");
            // Notify client of where the result data is stored
            //response.setHeader("X-Swarm-Result-Cloud", "bigquery");

            String bigqueryDestinationTableQualified = String.format("%s.%s.%s",
                bigQueryClient.getProjectName(), bigqueryDestinationDataset, bigqueryDestinationTable);
            //response.setHeader("X-Swarm-Result-Table", bigqueryDestinationTableQualified);
            swarmTableIdentifier.databaseType = "bigquery";
            swarmTableIdentifier.databaseName = bigQueryClient.getDatasetName();
            swarmTableIdentifier.tableName = bigqueryDestinationTable;
//            PrintWriter responseWriter = response.getWriter();
//            log.info("Writing data to response stream");
//
//            for (FieldValueList fvl : tr.iterateAll()) {
//                responseWriter.println(String.format(
//                        "%s,%d,%d,%s,%s,%f,%d",
//                        fvl.get("reference_name").getStringValue(),
//                        fvl.get("start_position").getLongValue(),
//                        fvl.get("end_position").getLongValue(),
//                        fvl.get("reference_bases").getStringValue(),
//                        fvl.get("alternate_bases").getStringValue(),
//                        fvl.get("minor_af").getDoubleValue(),
//                        fvl.get("allele_count").getLongValue()
//                ));
//            }
//            log.info("Finished writing response");
            log.info("Deleting merged table using table result");
            bigQueryClient.deleteTableFromTableResult(tr);

            return swarmTableIdentifier;
        } else {
            log.info("Performing rest of computation in Athena");
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

            // join the tables together
            String mergeSql = "select\n" +
                    "  a.reference_name, \n" +
                    "  a.start_position,\n" +
                    "  a.end_position,\n" +
                    "  a.reference_bases,\n" +
                    "  a.alternate_bases,\n" +
                    "  (\n" +
                    "    (sum(coalesce(a.allele_count, 0)) + sum(coalesce(b.allele_count, 0))) \n" +
                    "     / \n" +
                    "    (\n" +
                    "      (sum(coalesce(cast(a.minor_af as double), 0)) \n" +
                    "       + sum(coalesce(cast(b.minor_af as double), 0)))\n" +
                    "       * \n" +
                    "      (\n" +
                    "        sum(coalesce(a.allele_count, 0)) + sum(coalesce(b.allele_count, 0))\n" +
                    "      )\n" +
                    "    )\n" +
                    "   ) as minor_af,\n" +
                    "  sum(coalesce(a.allele_count, 0)) + sum(coalesce(b.allele_count, 0)) as allele_count\n" +
                    "from \n" +
                    "  swarm.%s a\n" +
                    "full outer join\n" +
                    "  swarm.%s b\n" +
                    "  on a.reference_name = b.reference_name\n" +
                    "  and a.start_position = b.start_position\n" +
                    "  and a.end_position = b.end_position\n" +
                    "  and a.reference_bases = b.reference_bases\n" +
                    "  and a.alternate_bases = b.alternate_bases\n" +
                    "group by\n" +
                    "(a.reference_name, a.start_position, a.end_position, a.reference_bases, a.alternate_bases)";
            // 1st placeholder is athena table name, second is tablename from imported bigquery table
            mergeSql = String.format(mergeSql, athenaDestinationTable, importedBigqueryTableName);
            log.info(String.format(
                    "Merging tables %s and %s and returning results",
                    athenaDestinationTable, importedBigqueryTableName));
            GetQueryResultsResult queryResultsResult = athenaClient.executeQueryToResultSet(mergeSql);
            log.info("Finished merge query in Athena");
            log.info("Writing response headers");
            //response.setStatus(200);
            //response.setHeader("Content-Type", "text/csv");
            // Notify client of where the result data is stored
            //response.setHeader("X-Swarm-Result-Cloud", "athena");
            String athenaDestinationTableQualified = String.format("%s.%s",
                    athenaDestinationDataset, athenaDestinationTable);
            //response.setHeader("X-Swarm-Result-Table", athenaDestinationTableQualified);
            com.amazonaws.services.athena.model.ResultSet rs = queryResultsResult.getResultSet();
            // iterate tokens
//            List<Row> rows = rs.getRows();
//            log.info("Writing data to response stream");
//
//            PrintWriter responseWriter = response.getWriter();
//            for (Row row : rows) {
//                List<Datum> data = row.getData();
//                responseWriter.println(String.format(
//                        "%s,%s,%s,%s,%s,%s,%s",
//                        data.get(0).getVarCharValue(),
//                        data.get(1).getVarCharValue(),
//                        data.get(2).getVarCharValue(),
//                        data.get(3).getVarCharValue(),
//                        data.get(4).getVarCharValue(),
//                        data.get(5).getVarCharValue(),
//                        data.get(6).getVarCharValue()
//                ));
//            }
//
//            log.info("Finished writing response");
            //return new TableDatabasePair("athena", athenaDestinationTableQualified);
            swarmTableIdentifier.databaseType = "athena";
            swarmTableIdentifier.databaseName = athenaDestinationDataset;
            swarmTableIdentifier.tableName = athenaDestinationTable;
            return swarmTableIdentifier;
        }
    }

    /**
     * Use Case 4: annotation
     */
    @RequestMapping(value = "/annotate/{gene_label}", method = {RequestMethod.GET})
    public void executeAnnotate(
            @PathVariable(required = false, name = "gene_label") String geneLabel,
            @RequestParam(required = true, name = "sourceCloud") String sourceCloudParam,
            @RequestParam(required = true, name = "tableName") String tableName,
            @RequestParam(required = true, name = "destinationTableName") String destinationTableName,
            //@RequestBody String body,
            HttpServletRequest request, HttpServletResponse response
    ) throws InterruptedException, IOException, TimeoutException, ExecutionException, SQLException {
        disallowQuoteSemicolonSpace(tableName);
//        final String MAIN_ROOT = System.getProperty("user.dir") + "/src/main/";
//        byte[] bytes = Files.readAllBytes(Paths.get(MAIN_ROOT + "sql/annotation.sql"));
//        String sql = new String(bytes, StandardCharsets.US_ASCII);
        String sql = loadSqlFile("annotation.sql");

        SwarmTableIdentifier swarmTableIdentifier;

        if (!StringUtils.isEmpty(geneLabel)) {
            if (!StringUtils.isEmpty(sourceCloudParam) || !StringUtils.isEmpty(tableName)) {
                throw new IllegalArgumentException("Cannot provide source table on gene based query");
            }
            GeneCoordinate geneCoordinate = getGeneCoordinates(geneLabel);
            if (geneCoordinate == null) {
                throw new IllegalArgumentException("Gene could not be found in coordinates table");
            }
            //getVariantsByGene(geneLabel);
            swarmTableIdentifier = this.getVariants(
                    sourceCloudParam,
                    geneCoordinate.referenceName,
                    geneCoordinate.startPosition.toString(),
                    geneCoordinate.endPosition.toString(),
                    null,
                    null,
                    "true");

        }

        if (sourceCloudParam.equals("athena")) {
            // This is just to create a new csv.gz dump of the input table
            String inputQuery = "select * from " + quoteAthenaTableIdentifier(tableName);
            S3ObjectId inputTable = athenaClient.executeQueryToObjectId(inputQuery);
            String tableDataUrl = String.format(
                    "s3://%s/%s", inputTable.getBucket(), inputTable.getKey());
            log.info("Executed query and got results object location: " + tableDataUrl);

            // move the results to bigquery
            S3DirectoryGzipConcatInputStream s3Stream =
                    new S3DirectoryGzipConcatInputStream(s3Client, tableDataUrl);
            String gcsImportDirectory = pathJoin(
                    bigQueryClient.getStorageBucket(),
                    "annotation-imports");
            String nonce = randomAlphaNumStringOfLength(12);
            gcsImportDirectory = pathJoin(gcsImportDirectory, nonce);
            String gcsImportFileUrl = pathJoin(gcsImportDirectory, "import.csv");
            GCSUploadStream gcsUploadStream =
                    new GCSUploadStream(gcsClient, gcsImportFileUrl);
            log.info("Transferring contents of " + tableDataUrl + " to " + gcsImportFileUrl);
            s3Stream.transferTo(gcsUploadStream);
            log.info("Finished transferring files in " + tableDataUrl + " to " + gcsImportFileUrl);


            // create a table from the directory where the Athena results were transferred to
            String importedAthenaTableName = "athena_import_" + nonce;
            //String headerLine = gcsClient.getFirstLineOfFile(gcsImportFileUrl);
            Table importedAthenaTable = bigQueryClient.createVariantTableFromGcs(
                    importedAthenaTableName, gcsImportDirectory);
            log.info("Created table from athena import: " + importedAthenaTableName);


            // run the annotation SQL on the imported table
            //String formattedTableName = quoteAthenaTableIdentifier(tableName);
            String formattedTableName = "`" + tableName + "`";
            String query = String.format(sql, formattedTableName);
            // ensure the destination table has a dataset, default to bigquery default dataset if not provided
            TableId destinationTableId = null;
            String[] terms = destinationTableName.split("\\.");
            if (terms.length == 1) {
                destinationTableName = String.format("%s.%s.%s",
                        bigQueryClient.getProjectName(), bigQueryClient.getDatasetName(), terms[0]);
                //destinationTableId = TableId.of(bigQueryClient.getDatasetName(), destinationTableName);
            } else if (terms.length == 2) {
                destinationTableName = String.format("%s.%s.%s",
                        bigQueryClient.getProjectName(), terms[0], terms[1]);
                //destinationTableId = TableId.of(terms[0], terms[1]);
            } else if (terms.length == 3) {
                destinationTableName = String.format("%s.%s.%s",
                        terms[0], terms[1], terms[2]);
                //destinationTableId = TableId.of(terms[0], terms[1], terms[2]);
            } else {
                throw new IllegalArgumentException("Unknown format for destination table name: " + destinationTableName);
            }

            // make this a CTAS query
            log.info("Creating variant annotation table " + destinationTableName +
                    " from variant table " + tableName);
            query = String.format(
                    "create table `%s` as (%s)",
                    destinationTableName, query
            );
            // TODO
            //TableResult tableResult = bigQueryClient.runSimpleQueryNoDestination(query);
            TableResult tableResult = bigQueryClient.runSimpleQueryNoDestination(query);
            //GetQueryResultsResult getQueryResultsResult = athenaClient.executeQueryToResultSet(query);
            response.setStatus(200);
            response.setHeader("X-Swarm-Result-Cloud", "bigquery");
            response.setHeader("X-Swarm-Result-Table",
                    String.format("%s.%s.%s",
                            bigQueryClient.getProjectName(),
                            bigQueryClient.getDatasetName(),
                            destinationTableName));

            // log transfer size

        } else if (sourceCloudParam.equals("bigquery")) {
            //bigQueryClient.runSimpleQuery(body);
            throw new UnsupportedOperationException("Cannot annotate tables in BigQuery yet");
        } else {
            throw new ValidationException("Unrecognized cloud param: " + sourceCloudParam);
        }
    }


    @RequestMapping("/hello")
    public String hello(@RequestParam(value="name") String name) {
        return String.format("Hello %s!", name);
    }

    /*
    @RequestMapping(
            value = "/variants/by_gene/{gene_label}",
            produces = "application/json",
            method = RequestMethod.GET)
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
        if (cloudParam.equals("bigquery") || cloudParam.equals("all")) {
            gcpFuture = (executorService.submit(gcpCallable));
        }
        Future<ResultSet> awsFuture = null;
        if (cloudParam.equals("athena") || cloudParam.equals("all")) {
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
            jsonWriter.name("bigquery");
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
            jsonWriter.name("athena");
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
//                jsonWriter.name("reference_name").value(data.get(i++).getVarCharValue());
//                jsonWriter.name("start_position").value(data.get(i++).getVarCharValue());
//                jsonWriter.name("end_position").value(data.get(i++).getVarCharValue());
//                jsonWriter.name("reference_bases").value(data.get(i++).getVarCharValue());
//                jsonWriter.name("alternate_bases").value(data.get(i++).getVarCharValue());
//                jsonWriter.name("minor_af").value(data.get(i++).getVarCharValue());
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
    */

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
        allowedClouds.add("athena");
        allowedClouds.add("bigquery");
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
