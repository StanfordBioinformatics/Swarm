package app.dao.client;

import app.AppLogging;
import app.dao.query.CountQuery;
import app.dao.query.VariantQuery;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.athena.model.*;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.simba.athena.jdbc.Driver;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import javax.validation.ValidationException;
import javax.validation.constraints.NotNull;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static app.dao.client.StringUtils.*;

public class AthenaClient {
    private static final Logger log = AppLogging.getLogger(AthenaClient.class);

    private AmazonAthena athena;
    private String databaseName;
    private Properties configuration;

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
        this.athena = athenaClientBuilder.build();

        this.databaseName = databaseName;
    }

    private Connection getConnection() throws SQLException {
        Properties connectionInfo = new Properties();
        connectionInfo.setProperty("User", this.configuration.getProperty(PROP_NAME_KEY_ID));
        connectionInfo.setProperty("Password", this.configuration.getProperty(PROP_NAME_SECRET_KEY));
        //connectionInfo.setProperty("S3OutputLocation", this.outputLocation);
        // DO NOT LOG jdbcUrl
        String jdbcUrl = "jdbc:awsathena://AwsRegion=us-east-2;";
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
        variantQuery.setUsePositionAsRange();
        variantQuery.setReferenceBases(referenceBases);
        variantQuery.setAlternateBases(alternateBases);
        variantQuery.setMinorAF(minorAF);
        variantQuery.setMinorAFMarginOfErrorPercentage(minorAFMarginOfErrorPercentage);

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
     * @param destinationDataset
     * @param destinationTable
     * @param deleteResultTable
     * @return
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
                        combinedDestTable);
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

    /**
     * Quotes Athena database/table identifier string by period delimited parts.
     * Athena only allows this syntax for SELECT statements.
     * <br>
     * eg: database.tablename becomes "database"."tablename"
     * @param s input table identifier
     */
    private String quoteAthenaTableIdentifier(String s) {
        if (s == null) { return null; }
        String[] terms = s.split("\\.");
        for (int i = 0; i < terms.length; i++) {
            terms[i] = "\"" + terms[i] + "\"";
        }
        return String.join(".", terms);
    }

    private void disallowQuoteSemicolonSpace(String s) {
        if (s.contains("'")) {
            throw new ValidationException("String cannot contain single quotes: " + s);
        }
        if (s.contains(";")) {
            throw new ValidationException("String cannot contain semicolon: " + s);
        }
        if (s.matches(".*\\s.*")) {
            throw new ValidationException("String cannot contain whitespace: " + s);
        }
    }


    public void createVariantTableFromS3(String shortTableName, String s3DirectoryUrl) {
        log.debug("createVariantTableFromS3(" + shortTableName + ", " + s3DirectoryUrl + ")");
        disallowQuoteSemicolonSpace(shortTableName);
        disallowQuoteSemicolonSpace(s3DirectoryUrl);
        String fullTableName = this.databaseName + "." + shortTableName;
        String query = "";
        List<String> fields = new ArrayList<>();
        fields.add("`reference_name` string");
        fields.add("`start_position` bigint");
        fields.add("`end_position` bigint");
        fields.add("`reference_bases` string");
        fields.add("`alternate_bases` string");
        fields.add("`minor_af` double");
        fields.add("`allele_count` bigint");
        String fieldString = String.join(",\n", fields);

        query += "CREATE EXTERNAL TABLE IF NOT EXISTS " +
                fullTableName +
                " (" + fieldString + ")\n" +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'\n" +
                "WITH SERDEPROPERTIES (\n" +
                "  'serialization.format' = ',',\n" +
                "  'field.delim' = ','\n" +
                ") LOCATION '" + s3DirectoryUrl + "'\n" +
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

    public StartQueryExecutionRequest variantQueryToStartQueryExecutionRequest(
            @NotNull VariantQuery variantQuery,
            @NotNull ResultConfiguration resultConfiguration,
            @NotNull String destinationTableName) {
        StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest();
        startQueryExecutionRequest.setResultConfiguration(resultConfiguration);

        StringBuilder sb = new StringBuilder();
        String quotedTable = quoteAthenaTableIdentifier(
                this.databaseName + "." + variantQuery.getTableIdentifier());
        String quotedDestTable = quoteAthenaTableIdentifier(destinationTableName);
        // https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html#ctas-table-properties
        String with = "with(format='TEXTFILE', field_delimiter=',')"; // automatically uses GZIP
        sb.append(String.format(
                "create table %s %s as select * from %s",
                quotedDestTable,
                with,
                quotedTable
        ));

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

    public com.amazonaws.services.athena.model.ResultSet executeQueryToResultSet(String query) {
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
        return getQueryResultsResult.getResultSet();
        //return rs;
    }

    /*@Override
    public Iterable<Map<String, Object>> executeQuery(String query) {
        return resultSetToIterableMap(executeQueryToResultSet(query));
    }*/

    private Iterable<Map<String,Object>> resultSetToIterableMap(ResultSet rs) {
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

    //@Override
    /*public long executeCount(String tableName, String fieldName, Object fieldValue) {
        String query = String.format(
                "select count(*) as ct"
                + " from %s.%s",
                this.databaseName,
                tableName
        );
        if (fieldName != null && fieldValue != null) {
            if (fieldValue instanceof Number) {
                query += String.format(
                        " where %s = %f",
                        fieldName, ((Number) fieldValue).doubleValue());
            } else {
                query += String.format(" where %s = \"%s\"", fieldName, fieldValue);
            }
        }
        Iterable<Map<String,Object>> results = executeQuery(query);
        String ct = results.iterator().next().get("ct").toString();
        System.out.println("ct: " + ct);
        return Long.valueOf(ct);
    }*/

    //@Override
    public long executeCount(CountQuery countQuery, String tableName) {
        return -1;
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
