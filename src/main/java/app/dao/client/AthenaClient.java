package app.dao.client;

import app.AppLogging;
import app.dao.query.CountQuery;
import app.dao.query.VariantQuery;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.athena.model.*;
import com.amazonaws.services.athena.model.ResultSet;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.google.gson.stream.JsonWriter;
import com.simba.athena.jdbc.Driver;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import javax.validation.ValidationException;
import javax.validation.constraints.NotNull;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static app.dao.client.StringUtils.*;

public class AthenaClient {
    private static final Logger log = AppLogging.getLogger(AthenaClient.class);

    private AmazonAthena athena;
    private String databaseName;
    private Properties configuration;
    private Region region = Region.getRegion(Regions.US_WEST_1);

    // prop names
    private final String PROP_NAME_KEY_ID = "accessKey";
    private final String PROP_NAME_SECRET_KEY = "secretKey";
    private final String PROP_NAME_OUTPUT_LOCATION = "outputLocationRoot";

    // athena query statuses
    private enum AthenaQueryStatus {
        // QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
        QUEUED("QUEUED"),
        RUNNING("RUNNING"),
        SUCCEEDED("SUCCEEDED"),
        FAILED("FAILED"),
        CANCELLED("CANCELLED");
        private String value;
        AthenaQueryStatus(String value) {
            this.value = value;
        }
        public String getValue() {
            return value;
        }
    }

    final static List<String> VCF_JOIN_COLUMNS = Arrays.asList(
        "reference_name",
        "start_position",
        "end_position",
        "reference_bases",
        "alternate_bases");

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

    //private HashMap<String,String> executionIdToLocation = new HashMap<>();
    private HashMap<String,String> tableNameToLocation = new HashMap<>();

    public AthenaClient(String databaseName, String credentialPath) {
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
        AmazonAthenaClientBuilder athenaClientBuilder = AmazonAthenaClientBuilder.standard().withCredentials(credProvider);
        athenaClientBuilder.setRegion(this.region.getName());
        this.athena = athenaClientBuilder.build();

        this.databaseName = databaseName;
    }

    public AmazonAthena getAthena() {
        return this.athena;
    }

    private Connection getConnection() throws SQLException {
        Properties connectionInfo = new Properties();
        connectionInfo.setProperty("User", this.configuration.getProperty(PROP_NAME_KEY_ID));
        connectionInfo.setProperty("Password", this.configuration.getProperty(PROP_NAME_SECRET_KEY));
        //connectionInfo.setProperty("S3OutputLocation", this.outputLocation);
        // DO NOT LOG jdbcUrl
        //String jdbcUrl = "jdbc:awsathena://AwsRegion=us-east-2;";
        String jdbcUrl = "jdbc:awsathena://AwsRegion=" + region.getName() + ";";
        jdbcUrl += String.format("S3OutputLocation=%s;", this.configuration.getProperty(PROP_NAME_OUTPUT_LOCATION));
        return new Driver().connect(jdbcUrl, connectionInfo);
        //return DriverManager.getConnection(jdbcUrl, connectionInfo);
    }

    public String getStorageBucket() {
        String path = this.configuration.getProperty(PROP_NAME_OUTPUT_LOCATION);
        String proto = "s3://";
        if (!path.startsWith(proto)) {
            throw new ValidationException("Improperly formatted output location property");
        }
        path = path.substring(proto.length());
        int slashIndex = path.indexOf("/");
        if (slashIndex > 0) {
            return proto + path.substring(0, slashIndex);
        } else {
            return proto + path;
        }
    }

    /**
     * Returns the S3 bucket path to output query results into. Includes "s3://" protocol segment.
     */
    public String getOutputLocation() {
        return this.configuration.getProperty(PROP_NAME_OUTPUT_LOCATION);
    }

    public PreparedStatement queryToPreparedStatement(String query) {
        try {
            log.debug("getting connection");
            Connection conn = getConnection();
            log.debug("preparing statement");
            PreparedStatement ps = conn.prepareStatement(query);
            log.debug("returning prepared statement");
            return ps;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @see AthenaClient#executeVariantQuery(VariantQuery, Optional, Optional, Optional) 
     *
     * @param referenceName
     * @param startPosition
     * @param endPosition
     * @param referenceBases
     * @param alternateBases
     * @param minorAF
     * @param minorAFMarginOfErrorPercentage
     * @param sourceTable name of table to query
     * @param destinationDataset
     * @param destinationTable
     * @param deleteResultTable delete the table after exporting results
     */
    public S3ObjectId executeVariantQuery(
            @Nullable String referenceName,
            @Nullable Long startPosition,
            @Nullable Long endPosition,
            @Nullable String referenceBases,
            @Nullable String alternateBases,
            @Nullable Double minorAF,
            @Nullable Double minorAFMarginOfErrorPercentage,
            @Nullable String rsid,
            //@NotNull String sourceDataset,
            @NotNull String sourceTable,
            @NotNull Optional<String> destinationDataset,
            @NotNull Optional<String> destinationTable,
            @NotNull Optional<Boolean> deleteResultTable) {
        if (minorAFMarginOfErrorPercentage != null) {
            assert (minorAFMarginOfErrorPercentage >= 0 && minorAFMarginOfErrorPercentage <= 1.0);
        }
        VariantQuery variantQuery = new VariantQuery();
        variantQuery.setReferenceName(referenceName);
        variantQuery.setStartPosition(startPosition);
        variantQuery.setEndPosition(endPosition);
        // use start and end as inclusive range
        variantQuery.setUsePositionAsRange(true);
        variantQuery.setReferenceBases(referenceBases);
        variantQuery.setAlternateBases(alternateBases);
        variantQuery.setMinorAF(minorAF);
        variantQuery.setMinorAFMarginOfErrorPercentage(minorAFMarginOfErrorPercentage);
        variantQuery.setRsid(rsid);
        variantQuery.setTableIdentifier(
                String.join(".", new String[]{this.databaseName, sourceTable}));
        return executeVariantQuery(
                variantQuery,
                //sourceTable,
                destinationDataset,
                destinationTable,
                deleteResultTable);
    }

    /**
     * This method does the following:
     * <br>
     * 1) Run a variant query with the provided parameters
     * <br>
     * 2) Export the results to an S3 bucket.
     * <br>
     * 3) Deletes the result table from Athena if `deleteResultTable` is true,
     * but not the results file(s)
     * <br>
     * 4) Returns an s3 object handle referencing the results directory. All files
     * in this s3 directory are gzipped results from this query.
     * 
     * @param variantQuery
     * //@param sourceTable
     * @param destinationDataset dataset to put results into
     * @param destinationTable table to put results into
     * @param deleteResultTable whether to delete table after query, does not delete data files
     * @return s3ObjectId for s3 directory where data files are stored
     */
    public S3ObjectId executeVariantQuery(
            @NotNull VariantQuery variantQuery,
            //@NotNull String sourceTable,
            @NotNull Optional<String> destinationDataset,
            @NotNull Optional<String> destinationTable,
            @NotNull Optional<Boolean> deleteResultTable) {
        String outputId = randomAlphaNumStringOfLength(32);
        // trailing slash important
        // https://docs.aws.amazon.com/athena/latest/ug/tables-location-format.html
        String outputLocation = ensureTrailingSlash(pathJoin(getOutputLocation(), outputId));
        log.info("Using output location: " + outputLocation);
        ResultConfiguration resultConfiguration = new ResultConfiguration()
                .withOutputLocation(outputLocation);

        String destinationDatasetStr = destinationDataset.isPresent() ?
                destinationDataset.get()
                : this.databaseName;
        String destinationTableStr = destinationTable.isPresent() ?
                destinationTable.get()
                : "variant_query_" + outputId;
        String combinedDestTable = destinationDatasetStr + "." + destinationTableStr;
        log.info("Query destination table: " + combinedDestTable);
        StartQueryExecutionRequest startQueryExecutionRequest =
                variantQueryToStartQueryExecutionRequest(
                        variantQuery,
                        resultConfiguration,
                        Optional.of(combinedDestTable));
        StartQueryExecutionResult startQueryExecutionResult =
                athena.startQueryExecution(startQueryExecutionRequest);
        String executionId = startQueryExecutionResult.getQueryExecutionId();
        log.info("Waiting for query execution " + executionId + " to finish");
        waitForQueryExecution(executionId, 5 * 60 * 1000);
        log.info("Query execution " + executionId + " finished");
//        GetQueryResultsRequest getQueryResultsRequest =
//                new GetQueryResultsRequest().withQueryExecutionId(executionId);
//        GetQueryResultsResult getQueryResultsResult =
//                this.athena.getQueryResults(getQueryResultsRequest);

        // delete table unless told not to
        if (deleteResultTable.isPresent() && !deleteResultTable.get()) {
            log.info("Not deleting result table: " + combinedDestTable);
        } else {
            this.deleteTable(destinationTableStr);
            log.info("Deleted result table: " + combinedDestTable);
        }

        String tableFilesUrl = pathJoin(outputLocation, "tables");
        tableFilesUrl = pathJoin(tableFilesUrl, executionId);
        log.info("Output results should be available in: " + tableFilesUrl);
        S3PathParser parser = new S3PathParser(tableFilesUrl);
        return new S3ObjectId(parser.bucket, parser.objectPath);
    }

    private String prepareSqlString(String s) {
        return "'" + escapeQuotes(s) + "'";
    }

    /**
     * Athena provides no native query parameterization. We can provide some measure of protection
     * for numeric types by putting them in a quoted string and casting back to a numeric type.
     * <br>
     * Since these casts are in the WHERE clause, not the SELECT, this adds minimal overhead.
     * @param l the long to treat as an Athena bigint type
     * @return
     */
    private String prepareSqlBigInt(long l) {
        return "cast(" + l + " as bigint)";
    }

    private String escapeQuotes(String s) {
        return s.replaceAll("'", "\\'");
    }


    public StartQueryExecutionRequest variantQueryToStartQueryExecutionRequest(
            @NotNull VariantQuery variantQuery,
            @NotNull ResultConfiguration resultConfiguration,
            @NotNull Optional<String> destinationTableName) {
        StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest();
        startQueryExecutionRequest.setResultConfiguration(resultConfiguration);

        StringBuilder sb = new StringBuilder();
        String quotedTable = quoteAthenaTableIdentifier(
                this.databaseName + "." + variantQuery.getTableIdentifier());
        // TODO specify format here
        //String with = "with(format='TEXTFILE', field_delimiter=',')"; // automatically uses GZIP
        String with = "with(format='PARQUET')";
        if (destinationTableName.isPresent()) {
            String quotedDestTable = quoteAthenaTableIdentifier(destinationTableName.get());
            // https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html#ctas-table-properties
            sb.append(String.format(
                    "create table %s %s as ",
                    quotedDestTable,
                    with
            ));
        }
        sb.append(String.format(
                "select %s from %s",
                variantQuery.getCountOnly() ? "count(*) as ct" : "*",
                quotedTable));

        List<String> wheres = new ArrayList<>();
        if (variantQuery.getReferenceName() != null) {
            wheres.add("reference_name = " + prepareSqlString(variantQuery.getReferenceName()));
        }
        // Position parameters
        if (variantQuery.getUsePositionAsRange()) {
            // range based is a special case
            String whereTerm =
                    " ((start_position >= %s and start_position <= %s)" // start pos overlaps gene
                            + " or (end_position >= %s and end_position <= %s)" // end pos overlaps gene
                            + " or (start_position < %s and end_position > %s))";
            String start = variantQuery.getStartPosition() != null ?
                    variantQuery.getStartPosition().toString()
                    : "0";
            String end = variantQuery.getEndPosition() != null ?
                    variantQuery.getEndPosition().toString()
                    : Long.valueOf(Long.MAX_VALUE).toString();
            whereTerm = String.format(whereTerm,
                    start, end, start, end, start, end);
            wheres.add(whereTerm);

        } else {
            if (variantQuery.getStartPosition() != null) {
                wheres.add("start_position "
                        + variantQuery.getStartPositionOperator()
                        + " " + prepareSqlBigInt(variantQuery.getStartPosition()));
            }
            if (variantQuery.getEndPosition() != null) {
                wheres.add("end_position "
                        + variantQuery.getEndPositionOperator()
                        + " " + prepareSqlBigInt(variantQuery.getEndPosition()));
            }
        }

        if (variantQuery.getReferenceBases() != null) {
            wheres.add("reference_bases = " + prepareSqlString(variantQuery.getReferenceBases()));
        }
        if (variantQuery.getAlternateBases() != null) {
            wheres.add("alternate_bases = " + prepareSqlString(variantQuery.getAlternateBases()));
        }

        if (variantQuery.getRsid() != null) {
            wheres.add("id = " + prepareSqlString(variantQuery.getRsid()));
        }

        // TODO add minorAF where clauses

        // construct where clause
        if (wheres.size() > 0) {
            sb.append(" where");
            for (int i = 0; i < wheres.size(); i++) {
                if (i > 0) {
                    sb.append(" and");
                }
                sb.append(" (").append(wheres.get(i)).append(")");
            }
        }

        String queryString = sb.toString();
        System.out.println("Query: " + queryString);
        startQueryExecutionRequest.setQueryString(queryString);
        return startQueryExecutionRequest;
    }


    /**
     * Creates a table with all string columns
     * @param shortTableName
     * @param s3DirectoryUrl
     * @param sourceFieldNames
     */
    public void createVariantTableFromS3(String shortTableName, String s3DirectoryUrl, List<String> sourceFieldNames) {
        log.debug("createVariantTableFromS3(" + shortTableName + ", " + s3DirectoryUrl + ")");
        disallowQuoteSemicolonSpace(shortTableName);
        disallowQuoteSemicolonSpace(s3DirectoryUrl);
        String fullTableName = this.databaseName + "." + shortTableName;
        String query = "";
        List<String> fields = new ArrayList<>();
        List<String> allowedIntegerColumns = Arrays.asList("start_position", "end_position");
        for (String s : sourceFieldNames) {
            if (allowedIntegerColumns.contains(s)) {
                fields.add(String.format("`%s` bigint", s));
            } else {
                fields.add(String.format("`%s` string", s));
            }
        }
//        List<String> fields = new ArrayList<>();
//        fields.add("`reference_name` string");
//        fields.add("`start_position` bigint");
//        fields.add("`end_position` bigint");
//        fields.add("`reference_bases` string");
//        fields.add("`alternate_bases` string");
//        fields.add("`minor_af` double");
//        fields.add("`allele_count` bigint");
        String fieldString = String.join(",\n", fields);

        query += "CREATE EXTERNAL TABLE IF NOT EXISTS " +
                fullTableName +
                " (" + fieldString + ")\n" +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'\n" +
                "WITH SERDEPROPERTIES (\n" +
                "  'serialization.format' = ',',\n" +
                "  'field.delim' = ','\n" +
                ") " +
                "LOCATION '" + s3DirectoryUrl + "'\n" +
                "TBLPROPERTIES ('has_encrypted_data'='false');";
        StartQueryExecutionRequest startRequest = new StartQueryExecutionRequest()
                .withResultConfiguration(
                        new ResultConfiguration()
                                .withOutputLocation(getOutputLocation()))
                .withQueryString(query);
        StartQueryExecutionResult startResult = athena.startQueryExecution(startRequest);
        waitForQueryExecution(startResult.getQueryExecutionId(), 5 * 60 * 1000);
        log.debug("Created variant table " + fullTableName + " from s3 directory: " + s3DirectoryUrl);
    }

    public boolean doesTableExist(String tableName) {
        disallowQuoteSemicolonSpace(tableName);
        ResultConfiguration resultConfiguration = new ResultConfiguration()
                .withOutputLocation(getOutputLocation());
        StartQueryExecutionRequest startRequest = new StartQueryExecutionRequest()
                .withQueryString("show tables in " + this.databaseName)
                .withResultConfiguration(resultConfiguration);
        StartQueryExecutionResult startResult = athena.startQueryExecution(startRequest);
        String executionId = startResult.getQueryExecutionId();
        waitForQueryExecution(executionId, 5 * 60 * 1000);
        GetQueryResultsRequest getRequest = new GetQueryResultsRequest()
                .withQueryExecutionId(executionId);
        GetQueryResultsResult getResult = athena.getQueryResults(getRequest);
        AtomicBoolean tableExists = new AtomicBoolean(false);
        List<Row> rows = getResult.getResultSet().getRows();
        for (Row row : rows) {
            for (Datum datum : row.getData()) {
                if (datum.getVarCharValue().equals(tableName)) {
                    tableExists.set(true);
                    break;
                }
            }
            if (tableExists.get()) break;
        }
        return tableExists.get();
    }

    public void deleteTable(String tableName) {
        deleteTable(this.databaseName, tableName);
    }

    private void deleteTable(String databaseName, String tableName) {
        ResultConfiguration resultConfiguration = new ResultConfiguration()
                .withOutputLocation(getOutputLocation());
        StartQueryExecutionRequest startRequest = new StartQueryExecutionRequest()
                .withQueryString("drop table if exists " + databaseName + "." + tableName)
                .withResultConfiguration(resultConfiguration);
        StartQueryExecutionResult startResult = athena.startQueryExecution(startRequest);
        String executionId = startResult.getQueryExecutionId();
        try {
            waitForQueryExecution(executionId, 5 * 60 * 1000);
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
        log.info("Finished deleting table");
    }

    /**
     * Waits timeoutMilliseconds until the BigQuery query with executionId has
     * completed, failed, or has been cancelled.
     * <br>
     * Caller can then look up the results.
     *
     * @param executionId id of this query execution in BigQuery
     * @param timeoutMilliseconds maximum number of milliseconds to wait. If exceeded, an
     *                            exception is thrown
     */
    private void waitForQueryExecution(String executionId, long timeoutMilliseconds) {
        // wait for completion
        // TODO make these parameters
        long intervalMillis = 2000;
        long maxWaitMillis = timeoutMilliseconds;
        long waitedMillis = 0;
        boolean done = false;
        while (!done) {
            GetQueryExecutionRequest getQueryExecutionRequest = new GetQueryExecutionRequest()
                    .withQueryExecutionId(executionId);
            GetQueryExecutionResult getQueryExecutionResult =
                    athena.getQueryExecution(getQueryExecutionRequest);
            String stateString = getQueryExecutionResult.getQueryExecution().getStatus().getState();
            AthenaQueryStatus status = AthenaQueryStatus.valueOf(stateString);
            switch (status) {
                case SUCCEEDED: {
                    done = true;
                    break;
                }
                case RUNNING: {
                    try {
                        Thread.currentThread().sleep(intervalMillis);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    break;
                }
                case QUEUED: {
                    // still waiting
                    System.out.println("Job still queued.");
                    break;
                }
                case CANCELLED: {
                    throw new RuntimeException("Job cancelled unexpectedly! " + getQueryExecutionResult.toString());
                }
                case FAILED: {
                    throw new RuntimeException("Job failed! " + getQueryExecutionResult.toString());
                }
            }
            waitedMillis += intervalMillis;
            if (!done && waitedMillis >= maxWaitMillis) {
                //throw new TimeOutException("Max wait timeout exceeded", maxWaitMillis);
                throw new RuntimeException("Max wait timeout exceeded: " + maxWaitMillis);
            }
        }
    }

    /**
     * Executes the sql string and returns a com.amazonaws.services.athena.model.ResultSet
     * <br>
     * TODO create converter class from com.amazonaws.services.athena.model.ResultSet to java.sql.ResultSet
     *
     * @param query sql string to execute
     */
    public GetQueryResultsResult executeQueryToResultSet(String query) {
        log.debug("executeQueryToResultSet: " + query);

        ResultConfiguration resultConfiguration = new ResultConfiguration()
                .withOutputLocation(getOutputLocation());
        StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest();
        startQueryExecutionRequest.setQueryString(query);
        startQueryExecutionRequest.setResultConfiguration(resultConfiguration);
        // begin query execution
        StartQueryExecutionResult startQueryExecutionResult =
                athena.startQueryExecution(startQueryExecutionRequest);
        String executionId = startQueryExecutionResult.getQueryExecutionId();

        waitForQueryExecution(executionId, 20 * 60 * 1000);
        GetQueryResultsRequest getQueryResultsRequest = new GetQueryResultsRequest()
                .withQueryExecutionId(executionId);

        // get results
        GetQueryResultsResult getQueryResultsResult = athena.getQueryResults(getQueryResultsRequest);

        // Write statistics to log
        GetQueryExecutionRequest getQueryExecutionRequest = new GetQueryExecutionRequest()
                .withQueryExecutionId(executionId);
        GetQueryExecutionResult getQueryExecutionResult = athena.getQueryExecution(getQueryExecutionRequest);
        QueryExecutionStatistics queryExecutionStatistics = getQueryExecutionResult.getQueryExecution().getStatistics();
        log.info("Athena bytes scanned: " + queryExecutionStatistics.getDataScannedInBytes().toString());
        //return getQueryResultsResult.getResultSet();

        return getQueryResultsResult;
        //return rs;
    }

    @Deprecated
    public void createTableAs(String query, String destinationTableName) {
//        throw new NotImplementedException("AthenaClient.createTableAs not yet implemented");

        query = "create table "
                + String.format("`%s`.`%s`", this.databaseName, destinationTableName)
                + " with(external_location=" + pathJoin(getOutputLocation(), destinationTableName) + ")"
                + " as " + query;
    }

    public S3ObjectId executeQueryToObjectId(@NotNull String query) {
        //String nonce = randomAlphaNumStringOfLength(16);
        //String outputDir = pathJoin(getOutputLocation(), nonce);
        String outputLocation = getOutputLocation();
        log.debug("Executing query and placing results in: " + outputLocation);
        ResultConfiguration resultConfiguration = new ResultConfiguration()
                .withOutputLocation(outputLocation);
        StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest();
        startQueryExecutionRequest.setQueryString(query);
        startQueryExecutionRequest.setResultConfiguration(resultConfiguration);
        // begin query execution
        StartQueryExecutionResult startQueryExecutionResult =
                athena.startQueryExecution(startQueryExecutionRequest);
        String executionId = startQueryExecutionResult.getQueryExecutionId();
        waitForQueryExecution(executionId, 20 * 60 * 1000);

        //GetQueryExecutionRequest getQueryExecutionRequest = new GetQueryExecutionRequest()
        //        .withQueryExecutionId(executionId);
        String tableFilesUrl = pathJoin(outputLocation, "tables");
        tableFilesUrl = pathJoin(tableFilesUrl, executionId);
        log.info("Output results should be available in: " + tableFilesUrl);
        S3PathParser parser = new S3PathParser(tableFilesUrl);
        return new S3ObjectId(parser.bucket, parser.objectPath);
    }

    public List<String> getTableColumns(String tableName) {
        String sql = "DESCRIBE " + String.format("`%s`.`%s`", databaseName, tableName) + ";";
        GetQueryResultsResult getQueryResultsResult = executeQueryToResultSet(sql);
        ResultSet rs = getQueryResultsResult.getResultSet();
        List<String> columns = new ArrayList<>();
        // DESCRIBE format:
        // <colname> <type>
        // ....
        // <empty row>
        // <partition information>
        for (Row row : rs.getRows()) {
            List<Datum> data = row.getData();
            if (data.size() > 1) {
                throw new RuntimeException("DESCRIBE row data had more than one entry: [" + row.toString() + "]");
            }
            String val = data.get(0).getVarCharValue();
            String[] vals = val.split("\\s+");
            if (vals.length == 0) {
                // end of column data
                break;
            }
            if (vals.length != 2) {
                throw new RuntimeException("DESCRIBE datum didn't have 2 terms: ["+ Arrays.toString(vals) +"]");
            }
            String colName = vals[0];
            //String type = vals[1];
            columns.add(colName);
        }
        return columns;
    }

    /**
     * Merges the provided schemas and removes duplicated column names
     * @param tableNameA schema A to merge with B
     * @param tableAliasA alias to use for table A
     * @param tableNameB schema B to merge with A
     * @param tableAliasB alias to use for table B
     * @return list of the merged column names, no duplicates
     */
    private List<String> mergeSchemaColumns(
            String tableNameA, String tableAliasA,
            String tableNameB, String tableAliasB) {
        String tablePrefixA = tableAliasA + ".";
        String tablePrefixB = tableAliasB + ".";

        List<String> listA = getTableColumns(tableNameA);
        List<String> listB = getTableColumns(tableNameB);

        List<String> listAWithPrefixes = listA
                .stream()
                .map(String::toLowerCase)
                .map(name -> tablePrefixA + name)
                .collect(Collectors.toList());
        List<String> listBWithPrefixes = listB
                .stream()
                .map(String::toLowerCase)
                .map(name -> tablePrefixB + name)
                .collect(Collectors.toList());

        return StringUtils.mergeColumnListsIgnorePrefixes(
                listAWithPrefixes, tablePrefixA,
                listBWithPrefixes, tablePrefixB,
                true);
    }

    public void mergeAndSerializeVcfTablesToJson(String tableName1, String tableName2, JsonWriter jsonWriter)
            throws IOException, InterruptedException {
        mergeAndSerializeVcfTablesToJson(tableName1, "a", tableName2, "b", jsonWriter);
    }

    public String getMergedVcfSelect(
            String tableName1, String table1Alias,
            String tableName2, String table2Alias) {
        return getMergedVcfSelect(tableName1, table1Alias,
                tableName2, table2Alias,
                DatabaseClientInterface.JoinType.FULL_JOIN);
    }

    public String getMergedVcfSelect(
            String tableName1, String table1Alias,
            String tableName2, String table2Alias,
            DatabaseClientInterface.JoinType joinType) {
        List<String> mergedColumnsNames = mergeSchemaColumns(tableName1, table1Alias, tableName2, table2Alias);
        String sql =
                "select @mergedColumnsNames " +
                        "from @dataset.@tableName1 as @table1Alias " +
                        joinType.getDefaultString() +
                        " @dataset.@tableName2 as @table2Alias";
        sql = sql.replace("@mergedColumnsNames", String.join(", ", mergedColumnsNames));
        sql = sql.replace("@dataset", this.databaseName);
        sql = sql.replace("@dataset", this.databaseName);
        sql = sql.replace("@tableName1", tableName1);
        sql = sql.replace("@table1Alias", table1Alias);
        sql = sql.replace("@tableName2", tableName2);
        sql = sql.replace("@table2Alias", table2Alias);

        // Add on clauses
        sql += " on ";
        boolean firstOn = true;
        for (String s : VCF_JOIN_COLUMNS) {
            if (!firstOn) {
                sql += " and ";
                firstOn = false;
            }
            firstOn = false;
            sql += String.format(" %s.%s = %s.%s ",
                    table1Alias, s, table2Alias, s);
        }

        return sql;
    }

    // TODO
    public void mergeAndSerializeVcfTablesToJson(
            String tableName1, String table1Alias,
            String tableName2, String table2Alias,
            JsonWriter jsonWriter) throws IOException, InterruptedException {
        //List<String> mergedColumnsNames = mergeSchemaColumns(schema1, table1Alias, schema2, table2Alias);
        String sql = getMergedVcfSelect(tableName1, table1Alias, tableName2, table2Alias);
        serializeVcfQueryToJson(sql, jsonWriter);
    }

    /**
     * Serializes a vcf-conformant query to json, combining the samples to allele counts and splitting alternates into
     * separate records.
     * @param query must conform to expected vcf table columns, with all other columns being assumed to be samples in the form
     *              "[0-INTMAX]+|[0-INTMAX]+"
     * @param jsonWriter writer to serialize the response into
     * @throws InterruptedException if interrupted
     * @throws IOException if error in writing
     */
    public void serializeVcfQueryToJson(String query, JsonWriter jsonWriter)
            throws InterruptedException, IOException {
        GetQueryResultsResult getQueryResultsResult = this.executeQueryToResultSet(query);
        ResultSet rs = getQueryResultsResult.getResultSet();
        List<Row> rows = rs.getRows();
        List<String> columnNames = new ArrayList<>();
        Row headerRow = rows.get(0);
        for (Datum datum : headerRow.getData()) {
            columnNames.add(datum.getVarCharValue());
        }

        // Any columns not in vcfColumnsList are assumed to be sample columns

        List<String> vcfColumnsList = Arrays.asList(vcfColumns);
        List<String> sampleColumnsList = new ArrayList<>();
        for (String s : columnNames) {
            if (!StringUtils.listContainsIgnoreCase(vcfColumnsList, s)) {
                sampleColumnsList.add(s);
            }
        }
        log.debug("Sample column headers: " + Arrays.toString(sampleColumnsList.toArray()));
        List<String> columnsToWrite = Arrays.asList(
                "reference_name", "start_position", "end_position", "id",
                "reference_bases", "alternate_bases", "allele_count", "af");

        jsonWriter.name("headers").beginArray();
        for (String columnName : columnsToWrite) {
            jsonWriter.value(columnName);
        }
        jsonWriter.endArray();

        jsonWriter.name("data").beginArray();
        Iterator<Row> rowIterator = rows.iterator();
        rowIterator.next(); // skip header row from athena
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            Function<String,String> getColVal =
                    (String colName) ->
                        row.getData().get(columnNames.indexOf(colName)).getVarCharValue();

            String[] alternateBases = getColVal.apply("alternate_bases").split(",");
            int altCount = alternateBases.length;

            int[] alleleCounts = new int[altCount + 1];
            Arrays.fill(alleleCounts, 0);

            // process sample columns
            for (String sampleColName : sampleColumnsList) {
                String sampleValue = getColVal.apply(sampleColName);
                if (sampleValue != null && sampleValue.length() > 0) {
                    String[] sampleGTs = sampleValue.split("\\|");
                    for (String gtString : sampleGTs) {
                        Long gtLong = StringUtils.toLongNullable(gtString);
                        if (gtLong != null) { // ignore errors
                            int gtInt = gtLong.intValue();
                            alleleCounts[gtInt] = alleleCounts[gtInt] + 1;
                        }
                    }
                }
            }
            // debug the counts
            System.out.println(String.format(
                    "reference_bases: %s, allele_count: %d",
                    getColVal.apply("reference_bases"), alleleCounts[0]));
            for (int i = 0; i < alternateBases.length; i++) {
                System.out.println(String.format(
                        "alternate_bases: %s, allele_count: %d",
                        alternateBases[i], alleleCounts[i+1]));
            }

            // Write out the data, one row per each alternate bases
            for (int i = 0; i < alternateBases.length; i++) {
                jsonWriter.beginArray();
                // do vcf columns in order
                jsonWriter.value(getColVal.apply("reference_name"));
                jsonWriter.value(StringUtils.toLongNullable(getColVal.apply("start_position")));
                jsonWriter.value(StringUtils.toLongNullable(getColVal.apply("end_position")));
                jsonWriter.value(getColVal.apply("id"));
                jsonWriter.value(getColVal.apply("reference_bases"));
                jsonWriter.value(alternateBases[i]);
                jsonWriter.value(alleleCounts[i+1]);
                double af = (double) alleleCounts[i+1] / (double) sampleColumnsList.size();
                jsonWriter.value(af);
                jsonWriter.endArray();
            }

        }
        jsonWriter.endArray();
    }

    /**
     * Assumes writer is placed at the appropriate location of the stream.
     *
     * Writes a table dictionary data object at the root of wherever writer is pointing.
     *
     * @param tableName table to query and serialize
     * @param jsonWriter JsonWriter to write JSON data into. This can wrap any Writer, for example StringWriter, HttpServletResponse.getWriter
     */
    public void serializeTableToJSON(String tableName, JsonWriter jsonWriter, boolean writeData) throws IOException {
        String query = String.format("select * from \"%s\".\"%s\"", this.databaseName, tableName);
        GetQueryResultsResult getQueryResultsResult = executeQueryToResultSet(query);
        com.amazonaws.services.athena.model.ResultSet rs = getQueryResultsResult.getResultSet();
        ResultSetMetadata resultSetMetadata = rs.getResultSetMetadata();
        List<ColumnInfo> columnInfos = resultSetMetadata.getColumnInfo();
        List<String> columnNames = new ArrayList<>();
        columnInfos.forEach((columnInfo) -> {
            columnNames.add(columnInfo.getName());
        });
        List<Row> rows = rs.getRows();

        //JsonWriter jsonWriter = new JsonWriter(writer);
        //jsonWriter.beginObject();
        jsonWriter.name("swarm_database_type").value("athena");
        jsonWriter.name("swarm_database_name").value(this.databaseName);
        jsonWriter.name("swarm_table_name").value(tableName);
        jsonWriter.name("data_count").value(rows.size());

        jsonWriter.name("headers").beginArray();
        for (String columnName : columnNames) {
            jsonWriter.value(columnName);
        }
        jsonWriter.endArray();

        if (!writeData) {
            jsonWriter.name("message").value("To return data in response, set return_results query parameter to true");
            return;
        }

        jsonWriter.name("data").beginArray();
        boolean isFirstRow = true;
        for (Row row : rows) {
            if (isFirstRow) { // Athena includes table headers inside the row data as the first row
                isFirstRow = false;
                continue;
            }
            // each row is an array of the column values
            jsonWriter.beginArray();
            List<Datum> data = row.getData();
            for (Datum datum : data) {
                String val = datum.getVarCharValue();
                Double d;
                Long l;
                // attempt to convert to numeric types
                if ((d = StringUtils.toDoubleNullable(val)) != null) {
                    jsonWriter.value(d);
                } else if ((l = StringUtils.toLongNullable(val)) != null) {
                    jsonWriter.value(l);
                } else {
                    jsonWriter.value(val);
                }
            }
            jsonWriter.endArray();
        }
        jsonWriter.endArray();
        //jsonWriter.endObject();
    }

    private Iterable<Map<String,Object>> resultSetToIterableMap(java.sql.ResultSet rs) {
        // get result set schema
        //List<ColumnInfo> columnInfoList = rs.getResultSetMetadata().getColumnInfo();
        ArrayList<String> fieldNames = new ArrayList<String>();
        //for (ColumnInfo ci : columnInfoList) {
        //    fieldNames.add(ci.getName());
        //}
        int colCount;
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            colCount = rsmd.getColumnCount();
            for (int i = 1; i <= colCount; i++) {
                fieldNames.add(rsmd.getColumnLabel(i));
            }

            List<Map<String,Object>> results = new ArrayList<Map<String,Object>>(/*rows.size()*/);
            while (rs.next()) {
                Map<String,Object> rowMap = new HashMap<String,Object>();
                for (String colLabel : fieldNames) {
                    rowMap.put(colLabel, rs.getString(colLabel));
                }

                /*Row row = rowsIterator.next();
                List<Datum> data = row.getData();
                // loop through columns inside row
                for (int i = 0; i < data.size(); i++) {
                    Datum d = data.get(i);
                    String value = d.getVarCharValue();
                    rowMap.put(fieldNames.get(i), value);
                    System.out.printf("%s = %s\n", fieldNames.get(i), value);
                }*/
                results.add(rowMap);
            }
            return results;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        //List<Row> rows = rs.getRows();
        //System.out.println("Result row count: " + rows.size());

        //Iterator<Row> rowsIterator = rows.iterator();
        // athena includes column headers in the result set, skip
        //rowsIterator.next();

    }


    public long executeCount(VariantQuery variantQuery) {
        if (!variantQuery.getCountOnly()) {
            throw new IllegalArgumentException("VariantQuery did not have countOnly set");
        }
        ResultConfiguration resultConfiguration = new ResultConfiguration()
                .withOutputLocation(getOutputLocation());
        StartQueryExecutionRequest startQueryExecutionRequest =
                this.variantQueryToStartQueryExecutionRequest(
                        variantQuery, resultConfiguration, Optional.empty());
        StartQueryExecutionResult startQueryExecutionResult =
                athena.startQueryExecution(startQueryExecutionRequest);
        String executionId = startQueryExecutionResult.getQueryExecutionId();
        this.waitForQueryExecution(executionId, 5 * 60 * 1000);
        GetQueryResultsRequest getQueryResultsRequest = new GetQueryResultsRequest()
                .withQueryExecutionId(executionId);
        GetQueryResultsResult getQueryResultsResult = athena.getQueryResults(getQueryResultsRequest);
        List<Row> rows = getQueryResultsResult
                .getResultSet()
                .getRows();
        // row 0 is header, skip and get 'ct' field (col 0)
        String ct = rows.get(1) // first data row
                .getData()
                .get(0) // column
                .getVarCharValue();
        return Long.parseLong(ct);
    }

    public PreparedStatement countQueryToAthenaStatement(
            CountQuery countQuery, String tableName
    ) {

        StringBuilder sb = new StringBuilder(String.format(
                "select count(*) as ct from %s.%s",
                databaseName, tableName
        ));

        List<String> wheres = new ArrayList<>();
        List<Object> whereValues = new ArrayList<>();
        List<Class> whereTypes = new ArrayList<>();

        if (countQuery.getReferenceName() != null) {
            wheres.add("reference_name = ?");
            whereValues.add(countQuery.getReferenceName());
            whereTypes.add(String.class);
        }
        if (countQuery.getStartPosition() != null) {
            wheres.add(String.format(
                    "start_position %s ?",
                    countQuery.isUsePositionAsRange() ? ">=" : "="
            ));
            whereValues.add(countQuery.getStartPosition());
            whereTypes.add(Long.class);
        }
        if (countQuery.getEndPosition() != null) {
            wheres.add(String.format(
                    "end_position %s ?",
                    countQuery.isUsePositionAsRange() ? "<=" : "="
            ));
            whereValues.add(countQuery.getEndPosition());
            whereTypes.add(Long.class);
        }
        if (countQuery.getReferenceBases() != null) {
            wheres.add("reference_bases = ?");
            whereValues.add(countQuery.getReferenceBases());
            whereTypes.add(String.class);
        }
        if (countQuery.getAlternateBases() != null) {
            wheres.add("alternate_bases = ?");
            whereValues.add(countQuery.getAlternateBases());
            whereTypes.add(String.class);
        }

        // construct and add where clause
        for (int i = 0; i < wheres.size(); i++) {
            if (i == 0) {
                sb.append(" where ");
            }
            if (i > 0) {
                sb.append(" and ");
            }
            sb.append(wheres.get(i));
        }

        /*Properties connectionInfo = new Properties();
        connectionInfo.setProperty("User", this.configuration.getProperty(PROP_NAME_KEY_ID));
        connectionInfo.setProperty("Password", this.configuration.getProperty(PROP_NAME_SECRET_KEY));
        //connectionInfo.setProperty("S3OutputLocation", this.outputLocation);
        // DO NOT LOG jdbcUrl
        String jdbcUrl = "jdbc:awsathena://AwsRegion=us-east-2;";
        jdbcUrl += String.format("S3OutputLocation=%s;", this.configuration.getProperty(PROP_NAME_OUTPUT_LOCATION));*/
        try (
                //Connection conn = DriverManager.getConnection(jdbcUrl, connectionInfo);
                Connection conn = getConnection();
                PreparedStatement ps = conn.prepareCall(sb.toString());
        ) {
            for (int i = 0; i < wheres.size(); i++) {
                if (whereTypes.get(i) == String.class) {
                    ps.setString(i + 1, (String) whereValues.get(i));
                } else if (whereTypes.get(i) == Long.class) {
                    ps.setLong(i + 1, (Long) whereValues.get(i));
                } else {
                    throw new IllegalArgumentException("PreparedStatement argument type must be Long or String");
                }
            }
            return ps;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


}
