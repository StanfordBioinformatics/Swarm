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

    final static String[] vcfColumns = new String[]{
            "reference_name",
            "start_position",
            "end_position",
            "id",
            "reference_bases",
            "alternate_bases",
            "qual",
            "filter",
            "info",
            "format"
    };

    // NOTE: bigquery identifiers are case sensitive, athena are not
    final static String athenaVcfTable = "thousandorig_half2_partitioned_bucketed";
    final static String bigqueryVcfTable = "thousandorig_half1_clustered";
    final static String athenaAnnotationTable = "hg19_variant_9b_table_part";
    final static String bigqueryAnnotationTable = "hg19_Variant_9B_Table";

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


    // TODO update for VCF table headers
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
                    variantQuery.setUsePositionAsRange(true);
                } else if (positionRange.equalsIgnoreCase("false")) {
                    variantQuery.setUsePositionAsRange(false);
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
    ) throws IOException, InterruptedException, TimeoutException, ExecutionException {
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
                Optional.empty(),
                geneCoordinate.referenceName,
                geneCoordinate.startPosition.toString(),
                geneCoordinate.endPosition.toString(),
                null,
                null,
                "true",
                null);

        if (swarmTableIdentifier == null) {
            throw new IllegalArgumentException("Failed to query for variants");
        }

        response.setHeader("Content-Type", "application/json");
        StringWriter stringWriter = new StringWriter();
        JsonWriter jsonWriter = new JsonWriter(stringWriter);
        jsonWriter.beginObject();

        if (swarmTableIdentifier.databaseType.equals("athena")) {
            //athenaClient.serializeTableToJSON(swarmTableIdentifier.tableName, jsonWriter, returnResults);
            //athenaClient.serializeMergedVcfTableToJson(swarmTableIdentifier.tableName, jsonWriter);
            athenaClient.serializeTableToJSON(swarmTableIdentifier.tableName, jsonWriter, returnResults);
        } else if (swarmTableIdentifier.databaseType.equals("bigquery")) {
            //bigQueryClient.serializeTableToJson(swarmTableIdentifier.tableName, jsonWriter, returnResults);
            //bigQueryClient.serializeMergedVcfTableToJson(swarmTableIdentifier.tableName, jsonWriter);
            bigQueryClient.serializeTableToJson(swarmTableIdentifier.tableName, jsonWriter, returnResults);

        } else {
            throw new IllegalStateException("Unknown databaseType " + swarmTableIdentifier.databaseType);
        }

        if (!returnResults) {
            jsonWriter.name("data_message").value("To return data in response, set return_results query parameter to true");
        }

        jsonWriter.endObject();

        String responseString = stringWriter.toString();
        response.setContentLengthLong(responseString.length());
        response.getWriter().write(responseString);
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

    private Map<String,List<String>> getSampleLocations()
            throws IOException, InterruptedException {
        String sql = loadSqlFile("superpopulation-samples.sql");
        List<String> half1List = new ArrayList<>();
        List<String> half2List = new ArrayList<>();
        TableResult tr = bigQueryClient.runSimpleQuery(sql);
        Iterable<FieldValueList> fvlIterator = tr.iterateAll();
        for (FieldValueList fvl : fvlIterator) {
            String sp = fvl.get("SuperPopulation").getStringValue();
            String half1 = fvl.get("Half1").getStringValue();
            String half2 = fvl.get("Half2").getStringValue();
            half1List.addAll(Arrays.asList(half1.split(",")));
            half2List.addAll(Arrays.asList(half2.split(",")));
        }
        Map<String,List<String>> retMap = new HashMap<>();
        retMap.put("half1", half1List);
        retMap.put("half2", half2List);
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
            //@RequestParam(required = false, name = "cloud", defaultValue = "all") String sourceCloud,
            @RequestParam(required = false, name = "reference_name") String referenceNameParam,
            @RequestParam(required = false, name = "start_position") String startPositionParam,
            @RequestParam(required = false, name = "end_position") String endPositionParam,
            @RequestParam(required = false, name = "reference_bases") String referenceBasesParam,
            @RequestParam(required = false, name = "alternate_bases") String alternateBasesParam,
            @RequestParam(required = false, name = "rsid") String rsidParam,
            @RequestParam(required = false, name = "position_range", defaultValue = "true") String positionRange,
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

        // Require some positional filter parameters
        // Make this more intelligent
        if (StringUtils.isEmpty(rsidParam)) {
            if (StringUtils.isEmpty(referenceNameParam)) {
                throw new ValidationException("Must provide a either an rsid or referenceName parameter");
            }
            if (StringUtils.isAnyEmpty(startPositionParam, endPositionParam)) {
                throw new ValidationException("Must provide either a start or end position filter");
            }
        }


        Callable<SwarmTableIdentifier> athenaCallable = new Callable<SwarmTableIdentifier>() {
            @Override
            public SwarmTableIdentifier call() throws Exception {
                return getVariants(
                        "athena",
                        Optional.empty(),
                        referenceNameParam,
                        startPositionParam,
                        endPositionParam,
                        referenceBasesParam,
                        alternateBasesParam,
                        positionRange,
                        rsidParam);
            }
        };
        Callable<SwarmTableIdentifier> bigqueryCallable = new Callable<SwarmTableIdentifier>() {
            @Override
            public SwarmTableIdentifier call() throws Exception {
                return getVariants(
                        "bigquery",
                        Optional.empty(),
                        referenceNameParam,
                        startPositionParam,
                        endPositionParam,
                        referenceBasesParam,
                        alternateBasesParam,
                        positionRange,
                        rsidParam);
            }
        };
        log.info("Submitting callables for athena and bigquery");
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<SwarmTableIdentifier> athenaFuture = executorService.submit(athenaCallable);
        Future<SwarmTableIdentifier> bigqueryFuture = executorService.submit(bigqueryCallable);
        int waitMinutes = 30;
        log.info("Initiating executor service shutdown and waiting " + waitMinutes + " for task completion");
        executorService.shutdown();
        executorService.awaitTermination(waitMinutes, TimeUnit.MINUTES);
        executorService.shutdown();
        log.info("Finished shutting down executor service");
        log.info("Getting athena future");
        SwarmTableIdentifier athenaTableIdentifier = athenaFuture.get(10, TimeUnit.MINUTES);
        log.info("Getting bigquery future");
        SwarmTableIdentifier bigqueryTableIdentifier = bigqueryFuture.get(10, TimeUnit.MINUTES);
        log.info("Finished getting future results");

        log.debug("Writing response headers");
        response.setHeader("Content-Type", "application/json");

        StringWriter stringWriter = new StringWriter();
        JsonWriter jsonWriter = new JsonWriter(stringWriter);
        jsonWriter.beginObject();

        log.info("Serializing athena table");
        jsonWriter.name("athena").beginObject();
        //athenaClient.serializeTableToJSON(athenaTableIdentifier.tableName, jsonWriter, returnResults);
        athenaClient.serializeVcfTableToJson(athenaTableIdentifier.tableName, jsonWriter);

        jsonWriter.endObject();

        log.info("Serializing bigquery table");
        jsonWriter.name("bigquery").beginObject();
        //bigQueryClient.serializeTableToJson(bigqueryTableIdentifier.tableName, jsonWriter, returnResults);
        bigQueryClient.serializeVcfTableToJson(bigqueryTableIdentifier.tableName, jsonWriter);
        jsonWriter.endObject();

        // serialize the table into the response, in JSON format.
//        if (swarmTableIdentifier.databaseType.equals("athena")) {
//            jsonWriter.name("athena").beginObject();
//            //athenaClient.serializeTableToJSON(swarmTableIdentifier.tableName, jsonWriter, returnResults);
//            //athenaClient.serializeMergedVcfTableToJson(swarmTableIdentifier.tableName, jsonWriter);
//        } else if (swarmTableIdentifier.databaseType.equals("bigquery")) {
//            jsonWriter.name("bigquery").beginObject();
//            //bigQueryClient.serializeTableToJson(swarmTableIdentifier.tableName, jsonWriter, returnResults);
//            //bigQueryClient.serializeMergedVcfTableToJson(swarmTableIdentifier.tableName, jsonWriter);
//            bigQueryClient.serializeVcfTableToJson(swarmTableIdentifier.tableName, jsonWriter);
//        } else {
//            throw new IllegalStateException("Unknown databaseType " + swarmTableIdentifier.databaseType);
//        }

        if (!returnResults) {
            jsonWriter.name("data_message").value("To return data in response, set return_results query parameter to true");
        }

        jsonWriter.endObject();

        String responseString = stringWriter.toString();
        response.setContentLength(responseString.length());
        response.getWriter().write(responseString);
    }

    public static class SwarmTableIdentifier {
        String databaseType; // athena, bigquery, etc
        String databaseName; // example: "swarm"
        String tableName; // within the database, all information needed to reference the table
        // Optional
        Long size = null;
        String storageUrl = null;
    }

    private SwarmTableIdentifier getVariants(
            String sourceDatabaseType,
            Optional<String> destinationDatabaseType,
            String referenceNameParam,
            String startPositionParam,
            String endPositionParam,
            String referenceBasesParam,
            String alternateBasesParam,
            String positionRange,
            String rsid) throws IOException, InterruptedException {
        String sourceDatabaseName = "swarm";
        String sourceTableName;
        if (sourceDatabaseType.equals("athena")) {
            sourceTableName = athenaVcfTable;
        } else if (sourceDatabaseType.equals("bigquery")) {
            sourceTableName = bigqueryVcfTable;
        } else {
            throw new ValidationException("Unknown sourceDatabaseType: " + sourceDatabaseType);
        }

        SwarmTableIdentifier swarmTableIdentifier = queryTableByVariantCoordinates(
                sourceDatabaseType,
                sourceDatabaseName,
                sourceTableName,
                destinationDatabaseType.isPresent() ?
                        destinationDatabaseType.get() :
                        sourceDatabaseType,
                referenceNameParam,
                startPositionParam,
                endPositionParam,
                referenceBasesParam,
                alternateBasesParam,
                positionRange,
                rsid);
        log.info("Finished call to queryTableByVariantCoordinates for variants from database type " + sourceDatabaseType);
        return swarmTableIdentifier;
    }

    /*
    private SwarmTableIdentifier getVariantsOld(
            String sourceCloud,
            String referenceNameParam,
            String startPositionParam,
            String endPositionParam,
            String referenceBasesParam,
            String alternateBasesParam,
            String positionRange,
            String rsid
    ) throws IOException, InterruptedException {
        VariantQuery athenaVariantQuery = new VariantQuery();
        VariantQuery bigqueryVariantQuery = new VariantQuery();
        boolean returnResults = false;
        try {
            if (!StringUtils.isEmpty(positionRange)) {
                if (positionRange.equalsIgnoreCase("true")) {
                    log.debug("Using positions as a range");
                    athenaVariantQuery.setUsePositionAsRange(true);
                    bigqueryVariantQuery.setUsePositionAsRange(true);
                } else if (positionRange.equalsIgnoreCase("false")) {
                    athenaVariantQuery.setUsePositionAsRange(false);
                    bigqueryVariantQuery.setUsePositionAsRange(false);
                } else {
                    throw new ValidationException("Invalid param for position_range, must be true or false");
                }
            }
            if (!StringUtils.isEmpty(sourceCloud)) {
                validateCloudParam(sourceCloud);
            }
            if (!StringUtils.isEmpty(referenceNameParam)) {
                validateReferenceName(referenceNameParam);
                athenaVariantQuery.setReferenceName(referenceNameParam);
                bigqueryVariantQuery.setReferenceName(referenceNameParam);
            }
            if (!StringUtils.isEmpty(startPositionParam)) {
                Long startPosition = validateLongString(startPositionParam);
                athenaVariantQuery.setStartPosition(startPosition);
                bigqueryVariantQuery.setStartPosition(startPosition);
            }
            if (!StringUtils.isEmpty(endPositionParam)) {
                Long endPosition = validateLongString(endPositionParam);
                athenaVariantQuery.setEndPosition(endPosition);
                bigqueryVariantQuery.setEndPosition(endPosition);
            }
            if (!StringUtils.isEmpty(referenceBasesParam)) {
                validateBasesString(referenceBasesParam);
                athenaVariantQuery.setReferenceBases(referenceBasesParam);
                bigqueryVariantQuery.setReferenceBases(referenceBasesParam);
            }
            if (!StringUtils.isEmpty(alternateBasesParam)) {
                validateBasesString(alternateBasesParam);
                athenaVariantQuery.setAlternateBases(alternateBasesParam);
                bigqueryVariantQuery.setAlternateBases(alternateBasesParam);
            }
            if (!StringUtils.isEmpty(rsid)) {
                athenaVariantQuery.setRsid(rsid);
                bigqueryVariantQuery.setRsid(rsid);
            }
        } catch (ValidationException e) {
            throw e;
        }

        athenaVariantQuery.setTableIdentifier(athenaVcfTable);
        bigqueryVariantQuery.setTableIdentifier(bigqueryVcfTable);

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
                        athenaVariantQuery,
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
                        bigqueryVariantQuery,
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
        if (doBigquery && doAthena) {
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
            long getTimeoutSeconds = 60 * 10; // 10 minutes
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

        Map<String,Map<String,List<String>>> superPopulationSamples =
                getSuperPopulationSamples(Optional.empty());

        boolean resultsInBigQuery = false, resultsInAthena = false;
        String bigqueryResultsTableFullName = null,
                athenaResultsTableFullName = null;
        SwarmTableIdentifier swarmTableIdentifier = new SwarmTableIdentifier();

        if (athenaResultSize < bigqueryResultSize) {
            log.info("Performing rest of computation in BigQuery");
            String athenaOutputId = getLastNonEmptySegmentOfPath(athenaResultDirectoryUrl);
            String gcsAthenaImportDirectory = pathJoin(
                    bigQueryClient.getStorageBucket(),
                    "athena-imports/" + athenaOutputId);
            String gcsAthenaImportFile = pathJoin(gcsAthenaImportDirectory, "import.parquet");
            log.info("Copying athena output id: " + athenaOutputId
                    + " from " + athenaResultDirectoryUrl
                    + " to " + gcsAthenaImportFile);
            GCSUploadStream gcsUploadStream = new GCSUploadStream(gcsClient, gcsAthenaImportFile);
            S3DirectoryConcatInputStream s3DirGzipInputStream =
                    new S3DirectoryConcatInputStream(s3Client, athenaResultDirectoryUrl);
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

            // Select the VCF columns and the sample columns for half2
            //List<String> athenaColumns = athenaClient.getTableColumns(athenaDestinationTable);
            List<String> athenaColumns = new ArrayList<>();
            athenaColumns.addAll(Arrays.asList(vcfColumns));
            for (String superPopulation : superPopulationSamples.get("half2").keySet()){
                athenaColumns.addAll(superPopulationSamples.get("half2").get(superPopulation));
            }
            Table importedAthenaTable = bigQueryClient.createVariantTableFromGcs(
                    importedAthenaTableName, Arrays.asList(gcsAthenaImportFile), athenaColumns);
            log.info("Finished creating table: " + importedAthenaTableName);

            String mergeSql = bigQueryClient.getMergedVcfSelect(
                    bigqueryDestinationTable, "a", importedAthenaTableName, "b");
            log.info(String.format(
                    "Merging tables %s and %s and returning results",
                    bigqueryDestinationTable, importedAthenaTableName));
            String bigqueryMergedTableName = "merge_" + nonce;
            TableId bigqueryMergedTableId = TableId.of(bigqueryDestinationDataset, bigqueryMergedTableName);
            // TODO
            TableResult tr = bigQueryClient.runSimpleQuery(mergeSql, Optional.of(bigqueryMergedTableId));
            //TableResult tr = bigQueryClient.runSimpleQuery(mergeSql);
            log.info("Finished merge query in BigQuery");
            log.info("Writing response headers");

            String bigqueryDestinationTableQualified = String.format("%s.%s.%s",
                    bigQueryClient.getProjectName(), bigqueryDestinationDataset, bigqueryDestinationTable);
            //response.setHeader("X-Swarm-Result-Table", bigqueryDestinationTableQualified);
            swarmTableIdentifier.databaseType = "bigquery";
            swarmTableIdentifier.databaseName = bigQueryClient.getDatasetName();
            swarmTableIdentifier.tableName = bigqueryDestinationTable;
            log.info("Deleting merged table using table result");
            //bigQueryClient.deleteTableFromTableResult(tr);
            return swarmTableIdentifier;
        } else {
            log.info("Performing rest of computation in Athena");
            String bigqueryOutputId = getLastNonEmptySegmentOfPath(bigqueryResultDirectoryUrl);
            String s3BigQueryImportDirectory = pathJoin(
                    athenaClient.getStorageBucket(),
                    "bigquery-imports/" + bigqueryOutputId + "/");
            String s3BigQueryImportFile = pathJoin(s3BigQueryImportDirectory, "import.parquet");
            log.info("Copying bigquery output id: " + bigqueryOutputId
                    + " from " + bigqueryResultDirectoryUrl
                    + " to " + s3BigQueryImportFile);
            S3UploadStream s3UploadStream = new S3UploadStream(s3Client, s3BigQueryImportFile);
            GCSDirectoryConcatInputStream gcsDirGzipInputStream =
                    new GCSDirectoryConcatInputStream(gcsClient, bigqueryResultDirectoryUrl);
            log.info("Initiating transfer");
            gcsDirGzipInputStream.transferTo(s3UploadStream);
            log.debug("Closing GCS input stream");
            gcsDirGzipInputStream.close();
            log.debug("Closing S3 output stream");
            s3UploadStream.close();
            log.debug("Finished transfer from GCS to S3");
            // create the table from the file uploaded
            // Include the VCF columns and sample columns from half1
            // List<String> bigqueryColumns = bigQueryClient.getTableColumns(bigqueryDestinationTable);
            List<String> bigqueryColumns = new ArrayList<>();
            bigqueryColumns.addAll(Arrays.asList(vcfColumns));
            for (String superPopulation : superPopulationSamples.get("half1").keySet()){
                bigqueryColumns.addAll(superPopulationSamples.get("half1").get(superPopulation));
            }

            String importedBigqueryTableName = "bigquery_import_" + bigqueryOutputId;
            log.info("Creating table " + importedBigqueryTableName + " from file " + s3BigQueryImportDirectory);
            athenaClient.createVariantTableFromS3(importedBigqueryTableName, s3BigQueryImportDirectory, bigqueryColumns);
            log.info("Finished creating table: " + importedBigqueryTableName);

            // join the tables together
            String mergedTableName = "merged_" + nonce;
            String mergeSql = athenaClient.getMergedVcfSelect(
                    athenaDestinationTable, "a", importedBigqueryTableName, "b");
            mergeSql = "create table "
                    + quoteAthenaTableIdentifier(athenaDestinationDataset + "." + mergedTableName)
                    //+ String.format("`%s`.`%s`", athenaDestinationDataset, mergedTableName)
                    + " as \n" + mergeSql;
            log.info(String.format(
                    "Merging tables %s and %s and returning results",
                    athenaDestinationTable, importedBigqueryTableName));
            GetQueryResultsResult queryResultsResult = athenaClient.executeQueryToResultSet(mergeSql);
            log.info("Finished merge query in Athena");
            log.info("Writing response headers");
            String athenaDestinationTableQualified = String.format("%s.%s",
                    athenaDestinationDataset, athenaDestinationTable);
            com.amazonaws.services.athena.model.ResultSet rs = queryResultsResult.getResultSet();
            swarmTableIdentifier.databaseType = "athena";
            swarmTableIdentifier.databaseName = athenaDestinationDataset;
            swarmTableIdentifier.tableName = mergedTableName;
            return swarmTableIdentifier;
        }
    }
     */

    /**
     * Queries a VCF table by given coordinate information.
     * Table column schema must match that expected by Swarm project
     *
     * @return identifier of the result location of the query
     * @throws IOException on IO error
     * @throws InterruptedException on unexpected interruption error
     */
    private SwarmTableIdentifier queryTableByVariantCoordinates(
            String sourceDatabaseType,
            String sourceDatabaseName, // TODO parametrize VariantQuery
            String sourceTableName,
            String destinationDatabaseType,
            String referenceNameParam,
            String startPositionParam,
            String endPositionParam,
            String referenceBasesParam,
            String alternateBasesParam,
            String positionRange,
            String rsid
    ) throws IOException, InterruptedException {
        VariantQuery variantQuery = new VariantQuery();
        boolean returnResults = false;
        try {
            if (!StringUtils.isEmpty(positionRange)) {
                if (positionRange.equalsIgnoreCase("true")) {
                    log.debug("Using positions as a range");
                    variantQuery.setUsePositionAsRange(true);
                } else if (positionRange.equalsIgnoreCase("false")) {
                    variantQuery.setUsePositionAsRange(false);
                } else {
                    throw new ValidationException("Invalid param for position_range, must be true or false");
                }
            }
            if (!StringUtils.isEmpty(sourceDatabaseType)) {
                validateCloudParam(sourceDatabaseType);
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
            if (!StringUtils.isEmpty(rsid)) {
                variantQuery.setRsid(rsid);
            }
        } catch (ValidationException e) {
            log.error("Failed to validate parameters");
            throw e;
        }

        variantQuery.setTableIdentifier(sourceTableName);

        String nonce = randomAlphaNumStringOfLength(12);
        String destinationDataset = "swarm";
        String destinationTableName = "genome_query_" + nonce;
        boolean deleteResultTable = false;
        SwarmTableIdentifier swarmTableIdentifier = new SwarmTableIdentifier();

        if (sourceDatabaseType.equals("athena")) {
            S3ObjectId s3ObjectId = getAthenaClient().executeVariantQuery(
                    variantQuery,
                    Optional.of(destinationDataset),
                    Optional.of(destinationTableName),
                    Optional.of(deleteResultTable));
            String athenaResultDirectoryUrl = s3Client.s3ObjectIdToString(s3ObjectId);
            long athenaResultSize = s3Client.getDirectorySize(athenaResultDirectoryUrl);

            if (destinationDatabaseType.equals("athena")) {
                log.info("Keeping results in athena");
                swarmTableIdentifier.databaseType = sourceDatabaseType;
                swarmTableIdentifier.databaseName = destinationDataset;
                swarmTableIdentifier.tableName = destinationTableName;
                swarmTableIdentifier.size = athenaResultSize;
                swarmTableIdentifier.storageUrl = athenaResultDirectoryUrl;
            } else if (destinationDatabaseType.equals("bigquery")) {
                log.info("Moving results to athena");
                String athenaOutputId = getLastNonEmptySegmentOfPath(athenaResultDirectoryUrl);
                String gcsAthenaImportDirectory = pathJoin(
                        bigQueryClient.getStorageBucket(),
                        "athena-imports/" + athenaOutputId);
                String gcsAthenaImportFile = pathJoin(gcsAthenaImportDirectory, "import.parquet");
                log.info("Copying athena output id: " + athenaOutputId
                        + " from " + athenaResultDirectoryUrl
                        + " to " + gcsAthenaImportFile);
                GCSUploadStream gcsUploadStream = new GCSUploadStream(gcsClient, gcsAthenaImportFile);
                S3DirectoryConcatInputStream s3DirGzipInputStream =
                        new S3DirectoryConcatInputStream(s3Client, athenaResultDirectoryUrl);
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
                List<String> athenaColumns = athenaClient.getTableColumns(sourceTableName);
                Table importedAthenaTable = bigQueryClient.createVariantTableFromGcs(
                        importedAthenaTableName, Arrays.asList(gcsAthenaImportFile), athenaColumns);
                log.info("Finished creating table: " + importedAthenaTableName);
                swarmTableIdentifier.databaseType = destinationDatabaseType;
                swarmTableIdentifier.databaseName = destinationDataset;
                swarmTableIdentifier.tableName = importedAthenaTableName;
                swarmTableIdentifier.size = athenaResultSize;
                swarmTableIdentifier.storageUrl = athenaResultDirectoryUrl;
            }
        } else if (sourceDatabaseType.equals("bigquery")) {
            BlobId blobId = getBigQueryClient().executeVariantQuery(
                    variantQuery,
                    Optional.of(destinationDataset),
                    Optional.of(destinationTableName),
                    Optional.of(deleteResultTable));
            String bigqueryResultDirectoryUrl = gcsClient.blobIdToString(blobId);
            long bigqueryResultSize = gcsClient.getDirectorySize(bigqueryResultDirectoryUrl);

            if (destinationDatabaseType.equals("bigquery")) {
                log.info("Keeping results in bigquery");
                swarmTableIdentifier.databaseType = sourceDatabaseType;
                swarmTableIdentifier.databaseName = destinationDataset;
                swarmTableIdentifier.tableName = destinationTableName;
                swarmTableIdentifier.size = bigqueryResultSize;
                swarmTableIdentifier.storageUrl = bigqueryResultDirectoryUrl;
            } else if (destinationDatabaseType.equals("athena")) {
                log.info("Moving results to athena");
                String bigqueryOutputId = getLastNonEmptySegmentOfPath(bigqueryResultDirectoryUrl);
                String s3BigQueryImportDirectory = pathJoin(
                        athenaClient.getStorageBucket(),
                        "bigquery-imports/" + bigqueryOutputId + "/");
                String s3BigQueryImportFile = pathJoin(s3BigQueryImportDirectory, "import.parquet");
                log.info("Copying bigquery output id: " + bigqueryOutputId
                        + " from " + bigqueryResultDirectoryUrl
                        + " to " + s3BigQueryImportFile);
                S3UploadStream s3UploadStream = new S3UploadStream(s3Client, s3BigQueryImportFile);
                GCSDirectoryConcatInputStream gcsDirGzipInputStream =
                        new GCSDirectoryConcatInputStream(gcsClient, bigqueryResultDirectoryUrl);
                log.info("Initiating transfer");
                gcsDirGzipInputStream.transferTo(s3UploadStream);
                log.debug("Closing GCS input stream");
                gcsDirGzipInputStream.close();
                log.debug("Closing S3 output stream");
                s3UploadStream.close();
                log.debug("Finished transfer from GCS to S3");
                List<String> bigqueryColumns = bigQueryClient.getTableColumns(sourceTableName);
                String importedBigqueryTableName = "bigquery_import_" + bigqueryOutputId;
                log.info("Creating table " + importedBigqueryTableName + " from file " + s3BigQueryImportDirectory);
                athenaClient.createVariantTableFromS3(importedBigqueryTableName, s3BigQueryImportDirectory, bigqueryColumns);
                log.info("Finished creating table: " + importedBigqueryTableName);
                swarmTableIdentifier.databaseType = destinationDatabaseType;
                swarmTableIdentifier.databaseName = destinationDataset;
                swarmTableIdentifier.tableName = importedBigqueryTableName;
                swarmTableIdentifier.size = bigqueryResultSize;
                swarmTableIdentifier.storageUrl = s3BigQueryImportDirectory;
            }
        } else {
            throw new IllegalArgumentException("Unrecognized value for destinationDatabaseType: " + destinationDatabaseType);
        }

        return swarmTableIdentifier;
    }

    private SwarmTableIdentifier getAnnotation(
            String sourceDatabaseType,
            Optional<String> destinationDatabaseType,
            String referenceNameParam,
            String startPositionParam,
            String endPositionParam,
            String referenceBasesParam,
            String alternateBasesParam,
            String positionRange,
            String rsid
    ) throws IOException, InterruptedException {
        String sourceDatabaseName = "swarm";
        String sourceTableName;
        if (sourceDatabaseType.equals("athena")) {
            sourceTableName = athenaAnnotationTable;
        } else if (sourceDatabaseType.equals("bigquery")) {
            sourceTableName = bigqueryAnnotationTable;
        } else {
            throw new ValidationException("Unknown sourceDatabaseType: " + sourceDatabaseType);
        }

        SwarmTableIdentifier swarmTableIdentifier = queryTableByVariantCoordinates(
                sourceDatabaseType,
                sourceDatabaseName,
                sourceTableName,
                destinationDatabaseType.isPresent() ?
                        destinationDatabaseType.get() :
                        sourceDatabaseType,
                referenceNameParam,
                startPositionParam,
                endPositionParam,
                referenceBasesParam,
                alternateBasesParam,
                positionRange,
                rsid);
        log.info("Finished call to queryTableByVariantCoordinates for annotation");
        return swarmTableIdentifier;
    }

    /*private SwarmTableIdentifier getAnnotationOld(
            String sourceDatabaseType,
            Optional<String> destinationDatabaseType,
            String referenceNameParam,
            String startPositionParam,
            String endPositionParam,
            String referenceBasesParam,
            String alternateBasesParam,
            String positionRange,
            String rsid
    ) throws IOException, InterruptedException {
        VariantQuery athenaVariantQuery = new VariantQuery();
        VariantQuery bigqueryVariantQuery = new VariantQuery();
        boolean returnResults = false;
        try {
            if (!StringUtils.isEmpty(positionRange)) {
                if (positionRange.equalsIgnoreCase("true")) {
                    log.debug("Using positions as a range");
                    athenaVariantQuery.setUsePositionAsRange(true);
                    bigqueryVariantQuery.setUsePositionAsRange(true);
                } else if (positionRange.equalsIgnoreCase("false")) {
                    athenaVariantQuery.setUsePositionAsRange(false);
                    bigqueryVariantQuery.setUsePositionAsRange(false);
                } else {
                    throw new ValidationException("Invalid param for position_range, must be true or false");
                }
            }
            if (!StringUtils.isEmpty(sourceDatabaseType)) {
                validateCloudParam(sourceDatabaseType);
            }
            if (!StringUtils.isEmpty(referenceNameParam)) {
                validateReferenceName(referenceNameParam);
                athenaVariantQuery.setReferenceName(referenceNameParam);
                bigqueryVariantQuery.setReferenceName(referenceNameParam);
            }
            if (!StringUtils.isEmpty(startPositionParam)) {
                Long startPosition = validateLongString(startPositionParam);
                athenaVariantQuery.setStartPosition(startPosition);
                bigqueryVariantQuery.setStartPosition(startPosition);
            }
            if (!StringUtils.isEmpty(endPositionParam)) {
                Long endPosition = validateLongString(endPositionParam);
                athenaVariantQuery.setEndPosition(endPosition);
                bigqueryVariantQuery.setEndPosition(endPosition);
            }
            if (!StringUtils.isEmpty(referenceBasesParam)) {
                validateBasesString(referenceBasesParam);
                athenaVariantQuery.setReferenceBases(referenceBasesParam);
                bigqueryVariantQuery.setReferenceBases(referenceBasesParam);
            }
            if (!StringUtils.isEmpty(alternateBasesParam)) {
                validateBasesString(alternateBasesParam);
                athenaVariantQuery.setAlternateBases(alternateBasesParam);
                bigqueryVariantQuery.setAlternateBases(alternateBasesParam);
            }
            if (!StringUtils.isEmpty(rsid)) {
                athenaVariantQuery.setRsid(rsid);
                bigqueryVariantQuery.setRsid(rsid);
            }
        } catch (ValidationException e) {
            throw e;
        }

        athenaVariantQuery.setTableIdentifier(athenaAnnotationTable);
        bigqueryVariantQuery.setTableIdentifier(bigqueryAnnotationTable);

        boolean forceDestinationBigquery = false;
        boolean forceDestinationAthena = false;
        if (destinationDatabaseType.isPresent()) {
            log.info("Forcing destination to " + destinationDatabaseType.get());
            if (destinationDatabaseType.get().equals("athena")) {
                forceDestinationAthena = true;
            } else if (destinationDatabaseType.get().equals("bigquery")) {
                forceDestinationBigquery = true;
            } else {
                throw new ValidationException("Destination database type must be one of {athena, bigquery}");
            }
        }

        String nonce = randomAlphaNumStringOfLength(12);
        String athenaDestinationDataset = "swarm";
        String athenaDestinationTable = "annotation_query_" + nonce;
        boolean athenaDeleteResultTable = false;
        String bigqueryDestinationDataset = athenaDestinationDataset;
        String bigqueryDestinationTable = athenaDestinationTable;
        boolean bigqueryDeleteResultTable = false;

        // This is an annotation query but executeVariantQuery includes all columns
        Callable<S3ObjectId> athenaCallable = new Callable<S3ObjectId>() {
            @Override
            public S3ObjectId call() throws Exception {
                S3ObjectId objectId = getAthenaClient().executeVariantQuery(
                        athenaVariantQuery,
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
                        bigqueryVariantQuery,
                        Optional.of(bigqueryDestinationDataset),
                        Optional.of(bigqueryDestinationTable),
                        Optional.of(bigqueryDeleteResultTable));
                log.info("Finished bigquery executeVariantQuery");
                return blobId;
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        boolean doBigquery = false, doAthena = false, doAll = false;
        if (sourceDatabaseType.equals("athena") || sourceDatabaseType.equals("all")) {
            doAthena = true;
        }
        if (sourceDatabaseType.equals("bigquery") || sourceDatabaseType.equals("all")) {
            doBigquery = true;
        }
        if (sourceDatabaseType.equals("all") || (doBigquery && doAthena)) {
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
        }

        log.debug("Getting query result locations");
        try {
            long getTimeoutSeconds = 60 * 10; // 10 minutes
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

//        Map<String,Map<String,List<String>>> superPopulationSamples =
//                getSuperPopulationSamples(Optional.empty());

        boolean resultsInBigQuery = false, resultsInAthena = false;
        String bigqueryResultsTableFullName = null,
                athenaResultsTableFullName = null;
        SwarmTableIdentifier swarmTableIdentifier = new SwarmTableIdentifier();

        if (athenaResultSize < bigqueryResultSize || forceDestinationBigquery) {
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

            // Select the VCF columns and the sample columns for half2
            //List<String> athenaColumns = athenaClient.getTableColumns(athenaDestinationTable);
            List<String> athenaColumns = athenaClient.getTableColumns(athenaDestinationTable);
            Table importedAthenaTable = bigQueryClient.createVariantTableFromGcs(
                    importedAthenaTableName, gcsAthenaImportDirectory, athenaColumns);
            log.info("Finished creating table: " + importedAthenaTableName);

            String mergeSql = bigQueryClient.getMergedVcfSelect(
                    bigqueryDestinationTable, "a", importedAthenaTableName, "b");
            log.info(String.format(
                    "Merging tables %s and %s and returning results",
                    bigqueryDestinationTable, importedAthenaTableName));
            String bigqueryMergedTableName = "merge_" + nonce;
            TableId bigqueryMergedTableId = TableId.of(bigqueryDestinationDataset, bigqueryMergedTableName);
            // TODO
            TableResult tr = bigQueryClient.runSimpleQuery(mergeSql, Optional.of(bigqueryMergedTableId));
            //TableResult tr = bigQueryClient.runSimpleQuery(mergeSql);
            log.info("Finished merge query in BigQuery");
            log.info("Writing response headers");

            String bigqueryDestinationTableQualified = String.format("%s.%s.%s",
                    bigQueryClient.getProjectName(), bigqueryDestinationDataset, bigqueryDestinationTable);
            swarmTableIdentifier.databaseType = "bigquery";
            swarmTableIdentifier.databaseName = bigQueryClient.getDatasetName();
            swarmTableIdentifier.tableName = bigqueryDestinationTable;
            log.info("Deleting merged table using table result");
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
            // Include the VCF columns and sample columns from half1
            List<String> bigqueryColumns = bigQueryClient.getTableColumns(bigqueryDestinationTable);

            String importedBigqueryTableName = "bigquery_import_" + bigqueryOutputId;
            log.info("Creating table " + importedBigqueryTableName + " from file " + s3BigQueryImportDirectory);
            athenaClient.createVariantTableFromS3(importedBigqueryTableName, s3BigQueryImportDirectory, bigqueryColumns);
            log.info("Finished creating table: " + importedBigqueryTableName);

            // join the tables together
            String mergedTableName = "merged_" + nonce;
            String mergeSql = athenaClient.getMergedVcfSelect(
                    athenaDestinationTable, "a", importedBigqueryTableName, "b");
            mergeSql = "create table "
                    + quoteAthenaTableIdentifier(athenaDestinationDataset + "." + mergedTableName)
                    //+ String.format("`%s`.`%s`", athenaDestinationDataset, mergedTableName)
                    + " as \n" + mergeSql;
            log.info(String.format(
                    "Merging tables %s and %s and returning results",
                    athenaDestinationTable, importedBigqueryTableName));
            GetQueryResultsResult queryResultsResult = athenaClient.executeQueryToResultSet(mergeSql);
            log.info("Finished merge query in Athena");
            log.info("Writing response headers");
            String athenaDestinationTableQualified = String.format("%s.%s",
                    athenaDestinationDataset, athenaDestinationTable);
            com.amazonaws.services.athena.model.ResultSet rs = queryResultsResult.getResultSet();
            swarmTableIdentifier.databaseType = "athena";
            swarmTableIdentifier.databaseName = athenaDestinationDataset;
            swarmTableIdentifier.tableName = mergedTableName;
            return swarmTableIdentifier;
        }
    }
    */

    /**
     * Use Case 4: annotation
     */
    @RequestMapping(value = "/annotate", method = {RequestMethod.GET})
    public void executeAnnotate(
            // Variant query parameters
            @RequestParam(required = false, name = "reference_name") String referenceNameParam,
            @RequestParam(required = false, name = "start_position") String startPositionParam,
            @RequestParam(required = false, name = "end_position") String endPositionParam,
            @RequestParam(required = false, name = "reference_bases") String referenceBasesParam,
            @RequestParam(required = false, name = "alternate_bases") String alternateBasesParam,
            @RequestParam(required = false, name = "rsid") String rsidParam,
            @RequestParam(required = false, name = "position_range", defaultValue = "true") String positionRange,
            @RequestParam(required = false, name = "gene") String geneLabel,
            
            // Annotation parameters
            @RequestParam(required = true, name = "variantsDatabaseType") String variantsDatabaseType,
            @RequestParam(required = true, name = "variantsDatabaseName") String variantsDatabaseName,
            @RequestParam(required = false /*TODO*/, name = "variantsTable") String variantsTableName,
            @RequestParam(required = true, name = "annotationDatabaseType") String annotationDatabaseType,
            @RequestParam(required = true, name = "annotationDatabaseName") String annotationDatabaseName,
            @RequestParam(required = false /*TODO*/, name = "annotationTable") String annotationTableName,
            @RequestParam(required = false, name = "destinationDatabaseType") String destinationDatabaseType,
            @RequestParam(required = true, name = "return_results", defaultValue = "true") String returnResultsParam,
            HttpServletRequest request, HttpServletResponse response
    ) throws InterruptedException, IOException, TimeoutException, ExecutionException, SQLException {
        boolean returnResults = false;
        if (!StringUtils.isEmpty(returnResultsParam)) {
            if (returnResultsParam.equals("true")) {
                returnResults = true;
            } else if (!returnResultsParam.equals("false")) {
                throw new ValidationException("Invalid param for return_results, must be true or false");
            }
        }
        boolean forceDestinationAthena = false;
        boolean forceDestinationBigquery = false;
        if (!StringUtils.isEmpty(destinationDatabaseType)) {
            if (destinationDatabaseType.equals("athena")) {
                forceDestinationAthena = true;
            } else if (destinationDatabaseType.equals("bigquery")) {
                forceDestinationBigquery = true;
            } else {
                throw new ValidationException("Unknown destinationDatabaseType: " + destinationDatabaseType);
            }
            log.info("Forcing annotation results to: " + destinationDatabaseType);
        }

        if (!variantsDatabaseName.equals("swarm") || !annotationDatabaseName.equals("swarm")) {
            throw new ValidationException("Currently only supporting dataset/databases named 'swarm'");
        }

        // validate geneLabel
        if (!StringUtils.isEmpty(geneLabel)) {
            if (!StringUtils.isAllEmpty(referenceNameParam, startPositionParam, endPositionParam,
                    referenceBasesParam, alternateBasesParam, rsidParam)) {
                throw new ValidationException("Cannot mix gene, rsid, and location based query parameters");
            }
        }
        // validate rsid
        if (!StringUtils.isEmpty(rsidParam)) {
            if (!StringUtils.isAllEmpty(referenceNameParam, startPositionParam, endPositionParam,
                    referenceBasesParam, alternateBasesParam, geneLabel)) {
                throw new ValidationException("Cannot mix gene, rsid, and location based query parameters");
            }
        }
        // validate database types
        List<String> acceptedDatabaseTypes = Arrays.asList("bigquery", "athena");
        if (!acceptedDatabaseTypes.containsAll(
                Arrays.asList(variantsDatabaseType, annotationDatabaseType, destinationDatabaseType))) {
            throw new ValidationException(
                    "variantsDatabaseType, annotationDatabaseType, destinationDatabaseType must be one of {athena, bigquery}");
        }

        log.warn("Temporarily disallowing variant and annotation from same database");
        if (variantsDatabaseType.equals(annotationDatabaseType)) {
            throw new ValidationException("Temporarily disallowing variants and annotations from same database");
        }

        if (!StringUtils.isAllEmpty(variantsTableName, annotationTableName)) {
            log.warn("Specifying variant and annotation table not yet supported, see getVariants and getAnnotation");
        }

        String variantsDestinationDatabaseType;
        if (!StringUtils.isEmpty(destinationDatabaseType)) {
            variantsDestinationDatabaseType = destinationDatabaseType;
        }

        disallowQuoteSemicolonSpace(variantsTableName);
        String sql = loadSqlFile("annotation.sql");

        SwarmTableIdentifier variantTableIdentifier;
        SwarmTableIdentifier annotationTableIdentifier;

        if (!StringUtils.isEmpty(geneLabel)) {
            log.debug("Performing gene based query for " + geneLabel);
            GeneCoordinate geneCoordinate = getGeneCoordinates(geneLabel);
            if (geneCoordinate == null) {
                throw new IllegalArgumentException("Gene could not be found in coordinates table " + geneLabel);
            }
            log.debug("Querying for variants at coordinates " +
                    String.format("chr%s:%d-%d",
                            geneCoordinate.referenceName,
                            geneCoordinate.startPosition,
                            geneCoordinate.endPosition));
            variantTableIdentifier = getVariants(
                    variantsDatabaseType,
                    Optional.of(destinationDatabaseType),
                    geneCoordinate.referenceName,
                    geneCoordinate.startPosition.toString(),
                    geneCoordinate.endPosition.toString(),
                    null,
                    null,
                    "true",
                    null);
            annotationTableIdentifier = getAnnotation(
                    annotationDatabaseType,
                    Optional.of(destinationDatabaseType),
                    geneCoordinate.referenceName,
                    geneCoordinate.startPosition.toString(),
                    geneCoordinate.endPosition.toString(),
                    null,
                    null,
                    "true",
                    null);
        } else if (!StringUtils.isEmpty(rsidParam)) {
            log.debug("Performing rsid based query");
            variantTableIdentifier = getVariants(
                    variantsDatabaseType,
                    Optional.of(destinationDatabaseType),
                    null,
                    null,
                    null,
                    null,
                    null,
                    positionRange,
                    rsidParam);
            annotationTableIdentifier = getAnnotation(
                    variantsDatabaseType,
                    Optional.of(destinationDatabaseType),
                    null,
                    null,
                    null,
                    null,
                    null,
                    positionRange,
                    rsidParam);
        } else {
            log.debug("Performing coordinates based query");
            log.debug("Querying for variants at coordinates " +
                    String.format("chr%s:%s-%s",
                            referenceNameParam,
                            startPositionParam,
                            endPositionParam));
            // TODO limit filter broadness to reduce read/transfer sizes
            variantTableIdentifier = getVariants(
                    variantsDatabaseType,
                    Optional.of(destinationDatabaseType),
                    referenceNameParam,
                    startPositionParam,
                    endPositionParam,
                    referenceBasesParam,
                    alternateBasesParam,
                    positionRange,
                    null);
            annotationTableIdentifier = getAnnotation(
                    variantsDatabaseType,
                    Optional.of(destinationDatabaseType),
                    referenceNameParam,
                    startPositionParam,
                    endPositionParam,
                    referenceBasesParam,
                    alternateBasesParam,
                    positionRange,
                    null);
        }

        List<String> variantColumns = null;
        List<String> annotationColumns = null;
        log.info("Checking whether a filtered table needs to be copied to another platform");
        if (variantTableIdentifier.databaseType.equals("athena")
                && annotationTableIdentifier.databaseType.equals("bigquery")) {
            // variants are in athena, annotation is in bigquery
            if (destinationDatabaseType.equals("athena")) {
                log.info("Variants are in athena, annotation is in bigquery, user requested results in athena");
                // Copy annotation results from bigquery to athena
                String bigqueryResultDirectoryUrl = annotationTableIdentifier.storageUrl;
                String athenaOutputId = getLastNonEmptySegmentOfPath(bigqueryResultDirectoryUrl);
                String bigqueryOutputId = getLastNonEmptySegmentOfPath(bigqueryResultDirectoryUrl);
                String s3BigQueryImportDirectory = pathJoin(
                        athenaClient.getStorageBucket(),
                        "bigquery-imports/" + bigqueryOutputId + "/");
                String s3BigQueryImportFile = pathJoin(s3BigQueryImportDirectory, "import.parquet");
                log.info("Copying bigquery output id: " + bigqueryOutputId
                        + " from " + bigqueryResultDirectoryUrl
                        + " to " + s3BigQueryImportFile);
                S3UploadStream s3UploadStream = new S3UploadStream(s3Client, s3BigQueryImportFile);
                GCSDirectoryConcatInputStream gcsDirGzipInputStream =
                        new GCSDirectoryConcatInputStream(gcsClient, bigqueryResultDirectoryUrl);
                log.info("Initiating transfer");
                gcsDirGzipInputStream.transferTo(s3UploadStream);
                log.debug("Closing GCS input stream");
                gcsDirGzipInputStream.close();
                log.debug("Closing S3 output stream");
                s3UploadStream.close();
                log.debug("Finished transfer from GCS to S3");
                // create the table from the file uploaded
                // Include the VCF columns and sample columns from half1
                annotationColumns = bigQueryClient.getTableColumns(annotationTableIdentifier.tableName);
                String importedBigqueryTableName = "bigquery_import_" + bigqueryOutputId;
                log.info("Creating table " + importedBigqueryTableName + " from file " + s3BigQueryImportDirectory);
                athenaClient.createVariantTableFromS3(importedBigqueryTableName, s3BigQueryImportDirectory, annotationColumns);
                log.info("Finished creating table: " + importedBigqueryTableName);
                annotationTableIdentifier.databaseType = destinationDatabaseType;
                annotationTableIdentifier.tableName = importedBigqueryTableName;
            } else if (annotationTableIdentifier.databaseType.equals("bigquery")) {
                log.info("Variants are in athena, annotation is in bigquery, user requested results in bigquery");
                // Copy variant results from bigquery to athena
                String athenaResultDirectoryUrl = variantTableIdentifier.storageUrl;
                String athenaOutputId = getLastNonEmptySegmentOfPath(athenaResultDirectoryUrl);
                String gcsAthenaImportDirectory = pathJoin(
                        bigQueryClient.getStorageBucket(),
                        "athena-imports/" + athenaOutputId);
                String gcsAthenaImportFile = pathJoin(gcsAthenaImportDirectory, "import.parquet");
                log.info("Copying athena output id: " + athenaOutputId
                        + " from " + athenaResultDirectoryUrl
                        + " to " + gcsAthenaImportFile);
                GCSUploadStream gcsUploadStream = new GCSUploadStream(gcsClient, gcsAthenaImportFile);
                S3DirectoryConcatInputStream s3DirGzipInputStream =
                        new S3DirectoryConcatInputStream(s3Client, athenaResultDirectoryUrl);
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

                // Select the VCF columns and the sample columns for half2
                variantColumns = athenaClient.getTableColumns(variantTableIdentifier.tableName);
                Table importedAthenaTable = bigQueryClient.createVariantTableFromGcs(
                        importedAthenaTableName, Arrays.asList(gcsAthenaImportFile), variantColumns);
                log.info("Finished creating table: " + importedAthenaTableName);
                log.info("Variants are in athena, annotation is in bigquery, user requested results in bigquery");
                variantTableIdentifier.databaseType = destinationDatabaseType;
                variantTableIdentifier.tableName = importedAthenaTableName;
            }
        } else if (variantTableIdentifier.databaseType.equals("bigquery")
                && annotationTableIdentifier.databaseType.equals("athena")) {
            // TODO This may be redundant because getVariants will already perform the transfer
            if (destinationDatabaseType.equals("bigquery")) {
                log.info("Variants are in athena, annotation is in bigquery, user requested results in bigquery");
                // Copy variant results from bigquery to athena
                String athenaResultDirectoryUrl = variantTableIdentifier.storageUrl;
                String athenaOutputId = getLastNonEmptySegmentOfPath(athenaResultDirectoryUrl);
                String gcsAthenaImportDirectory = pathJoin(
                        bigQueryClient.getStorageBucket(),
                        "athena-imports/" + athenaOutputId);
                String gcsAthenaImportFile = pathJoin(gcsAthenaImportDirectory, "import.parquet");
                log.info("Copying athena output id: " + athenaOutputId
                        + " from " + athenaResultDirectoryUrl
                        + " to " + gcsAthenaImportFile);
                GCSUploadStream gcsUploadStream = new GCSUploadStream(gcsClient, gcsAthenaImportFile);
                S3DirectoryConcatInputStream s3DirGzipInputStream =
                        new S3DirectoryConcatInputStream(s3Client, athenaResultDirectoryUrl);
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

                // Select the VCF columns and the sample columns for half2
                variantColumns = athenaClient.getTableColumns(variantTableIdentifier.tableName);
                Table importedAthenaTable = bigQueryClient.createVariantTableFromGcs(
                        importedAthenaTableName, Arrays.asList(gcsAthenaImportFile), variantColumns);
                log.info("Finished creating table: " + importedAthenaTableName);
                log.info("Variants are in athena, annotation is in bigquery, user requested results in bigquery");
                variantTableIdentifier.databaseType = destinationDatabaseType;
                variantTableIdentifier.tableName = importedAthenaTableName;
            } else if (annotationTableIdentifier.databaseType.equals("athena")) {
                log.info("Variants are in athena, annotation is in bigquery, user requested results in athena");
                // Copy annotation results from bigquery to athena
                String bigqueryResultDirectoryUrl = variantTableIdentifier.storageUrl;
                String bigqueryOutputId = getLastNonEmptySegmentOfPath(bigqueryResultDirectoryUrl);
                String s3BigQueryImportDirectory = pathJoin(
                        athenaClient.getStorageBucket(),
                        "bigquery-imports/" + bigqueryOutputId + "/");
                String s3BigQueryImportFile = pathJoin(s3BigQueryImportDirectory, "import.parquet");
                log.info("Copying bigquery output id: " + bigqueryOutputId
                        + " from " + bigqueryResultDirectoryUrl
                        + " to " + s3BigQueryImportFile);
                S3UploadStream s3UploadStream = new S3UploadStream(s3Client, s3BigQueryImportFile);
                GCSDirectoryConcatInputStream gcsDirGzipInputStream =
                        new GCSDirectoryConcatInputStream(gcsClient, bigqueryResultDirectoryUrl);
                log.info("Initiating transfer");
                gcsDirGzipInputStream.transferTo(s3UploadStream);
                log.debug("Closing GCS input stream");
                gcsDirGzipInputStream.close();
                log.debug("Closing S3 output stream");
                s3UploadStream.close();
                log.debug("Finished transfer from GCS to S3");
                // create the table from the file uploaded
                // Include the VCF columns and sample columns from half1
                annotationColumns = bigQueryClient.getTableColumns(annotationTableIdentifier.tableName);
                String importedBigqueryTableName = "bigquery_import_" + bigqueryOutputId;
                log.info("Creating table " + importedBigqueryTableName + " from file " + s3BigQueryImportDirectory);
                athenaClient.createVariantTableFromS3(importedBigqueryTableName, s3BigQueryImportDirectory, annotationColumns);
                log.info("Finished creating table: " + importedBigqueryTableName);
                annotationTableIdentifier.databaseType = destinationDatabaseType;
                annotationTableIdentifier.tableName = importedBigqueryTableName;
            }
        } /*else {
            throw new IllegalArgumentException("Annotating a table within the same database source is not implemented yet");
        }*/

        if (annotationColumns == null) {
            log.info("Looking up annotation table schema");
            if (annotationTableIdentifier.databaseType.equals("athena")) {
                annotationColumns = athenaClient.getTableColumns(annotationTableIdentifier.tableName);
            } else if (annotationTableIdentifier.databaseType.equals("bigquery")) {
                annotationColumns = bigQueryClient.getTableColumns(annotationTableIdentifier.tableName);
            } else {
                throw new IllegalStateException("Unknown annotation databaseType");
            }
        }
        if (variantColumns == null) {
            log.info("Looking up variant table schema");
            if (variantTableIdentifier.databaseType.equals("athena")) {
                variantColumns = athenaClient.getTableColumns(variantTableIdentifier.tableName);
            } else if (variantTableIdentifier.databaseType.equals("bigquery")) {
                variantColumns = bigQueryClient.getTableColumns(variantTableIdentifier.tableName);
            } else {
                throw new IllegalStateException("Unknown annotation databaseType");
            }
        }


        log.info(String.format("Performing annotation in %s", destinationDatabaseType));
//        String mergeSql =
//                "select @mergedColumnList "
//                        + "from @variantTable @variantAlias "
//                        + "full outer join @annotationTable @annotationAlias";
        String variantAlias = "a", annotationAlias = "b";
//        String variantPrefix = variantAlias + ".", annotationPrefix = annotationAlias + ".";
//        List<String> variantColumnsWithPrefixes = variantColumns.stream()
//                .map(col -> variantPrefix + col)
//                .collect(Collectors.toList());
//        List<String> annotationColumnsWithPrefixes = annotationColumns.stream()
//                .map(col -> annotationPrefix + col)
//                .collect(Collectors.toList());
//        List<String> mergedColumns = StringUtils.mergeColumnListsIgnorePrefixes(
//                variantColumnsWithPrefixes, variantPrefix,
//                annotationColumnsWithPrefixes, annotationPrefix,
//                true);
//        String mergedColumnListString = String.join(", ", mergedColumns);
        // TODO maybe change to StringBuilder, col list could be large, resulting in large mem copies here
        // Replace col list, and aliases. Table name formats depend on database, so do those later
//        mergeSql = mergeSql.replace("@variantAlias", variantAlias);
//        mergeSql = mergeSql.replace("@annotationAlias", annotationAlias);
//        mergeSql = mergeSql.replace("@mergedColumnList", mergedColumnListString);

        String destTable = "swarm_annotation_" + randomAlphaNumStringOfLength(10);
        log.info("Creating annotation table " + destTable);

        response.setHeader("Content-Type", "application/json");
        StringWriter stringWriter = new StringWriter();
        JsonWriter jsonWriter = new JsonWriter(stringWriter);
        jsonWriter.beginObject();

        if (destinationDatabaseType.equals("athena")) {
            String mergedVcfSelect = athenaClient.getMergedVcfSelect(
                    variantTableIdentifier.tableName, variantAlias,
                    annotationTableIdentifier.tableName, annotationAlias,
                    DatabaseClientInterface.JoinType.LEFT_JOIN
            );
//            mergeSql = mergeSql.replace("@mergedColumnList", mergedVcfSelect);
            log.info(String.format("Annotating variant table %s with annotation table %s in athena",
                    variantTableIdentifier.tableName, annotationTableIdentifier.tableName));
//            mergeSql = mergeSql.replace("@variantTable",
//                    quoteAthenaTableIdentifier(variantTableIdentifier.databaseName+"."+variantTableIdentifier.tableName));
//            mergeSql = mergeSql.replace("@annotationTable",
//                    quoteAthenaTableIdentifier(annotationTableIdentifier.databaseName+"."+annotationTableIdentifier.tableName));
            log.debug("Adding CTAS to annotation sql");
            String location = pathJoin(athenaClient.getStorageBucket(), destTable) + "/";
            String ctas = "create table " + String.format("\"%s\".\"%s\"", annotationTableIdentifier.databaseName, destTable) + "\n"
                    + "with(external_location='" + location + "')\n"
                    + "as\n"
                    + mergedVcfSelect;
            log.info("Running annotation merge query in athena");
            athenaClient.executeQueryToResultSet(ctas);
            log.info("Running serialize of " + destTable);
            athenaClient.serializeTableToJSON(destTable, jsonWriter, true);
        } else if (destinationDatabaseType.equals("bigquery")) {
            String mergedVcfSelect = bigQueryClient.getMergedVcfSelect(
                    variantTableIdentifier.tableName, variantAlias,
                    annotationTableIdentifier.tableName, annotationAlias,
                    DatabaseClientInterface.JoinType.LEFT_JOIN
            );
//            mergeSql = mergeSql.replace("@mergedColumnList", mergedVcfSelect);
            log.info(String.format("Annotating variant table %s with annotation table %s in bigquery",
                    variantTableIdentifier.tableName, annotationTableIdentifier.tableName));
//            mergeSql = mergeSql.replace("@variantTable",
//                    String.format("`%s.%s`",
//                            variantTableIdentifier.databaseName,
//                            variantTableIdentifier.tableName));
//            mergeSql = mergeSql.replace("@annotationTable",
//                    String.format("`%s.%s`",
//                            annotationTableIdentifier.databaseName,
//                            annotationTableIdentifier.tableName));
            log.info("Running annotation merge query in bigquery");
            TableResult tr = bigQueryClient.runSimpleQuery(
                    mergedVcfSelect,
                    Optional.of(TableId.of(annotationTableIdentifier.databaseName, destTable)));
            log.info("Running serialize of " + destTable);
            bigQueryClient.serializeTableToJson(destTable, jsonWriter, true);
        } else {
            throw new IllegalStateException("Unknown error occurred");
        }
        jsonWriter.endObject();

        String responseString = stringWriter.toString();
        response.setContentLength(responseString.length());
        response.getWriter().write(responseString);
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
