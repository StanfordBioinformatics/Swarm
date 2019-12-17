package app.dao.client;

import app.AppLogging;
import app.dao.query.CountQuery;
import app.dao.query.VariantQuery;
import com.google.api.client.util.ArrayMap;
import com.google.api.services.bigquery.model.TableReference;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.BlobId;
import com.google.gson.stream.JsonWriter;
import org.apache.logging.log4j.Logger;
import org.threeten.bp.Duration;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static app.dao.client.StringUtils.*;

public class BigQueryClient implements DatabaseClientInterface {
    private final Logger log = AppLogging.getLogger(BigQueryClient.class);
    private int BIGQUERY_INTERVAL_TABLE_EXPORT = 1; // seconds
    private int BIGQUERY_TIMEOUT_TABLE_EXPORT = 3; // minutes

    private RetryOption[] retryOptions = {
            RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
            RetryOption.totalTimeout(Duration.ofMinutes(3))
    };

    private BigQuery bigquery;
    private String projectName;
    private String datasetName;

    public BigQueryClient(
            String datasetName,
            String credentialFilePath) {
        BigQueryOptions.Builder bqb = BigQueryOptions.newBuilder();
        try {
            bqb.setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(credentialFilePath)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.bigquery = bqb.build().getService();

        this.datasetName = datasetName;
        this.projectName = this.bigquery.getOptions().getProjectId();

        // validate datasetName
        Dataset d = bigquery.getDataset(datasetName);
        if (d == null || !d.exists()) {
            throw new IllegalArgumentException(String.format(
                    "No dataset with name [%s] exists in project [%s]",
                    datasetName, projectName));
        }
    }

    public String getProjectName() {
        return this.projectName;
    }

    public String getDatasetName() {
        return this.datasetName;
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
     * @param sourceTable
     * @param destinationDataset
     * @param destinationTable
     * @param deleteResultTable
     */
    public BlobId executeVariantQuery(
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
            assert(minorAFMarginOfErrorPercentage >= 0 && minorAFMarginOfErrorPercentage <= 1.0);
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

        variantQuery.setTableIdentifier(sourceTable);
        return executeVariantQuery(
                variantQuery,
                destinationDataset,
                destinationTable,
                deleteResultTable);
    }

    /**
     * This method does the following:
     * <br>
     * 1) Run a variant query with the provided parameters
     * <br>
     * 2) Export the results to an GCS bucket.
     * <br>
     * 3) Deletes the result table from BigQuery if `deleteResultTable` is true,
     * but not the results file(s)
     * <br>
     * 4) Returns an gcs object handle referencing the results directory. All files
     * in this gcs directory are gzipped results from this query.
     *
     * @param variantQuery
     * //@param sourceTable
     * @param destinationDataset
     * @param destinationTable
     * @param deleteResultTable
     * @return
     */
    public BlobId executeVariantQuery(
            @NotNull VariantQuery variantQuery,
            @NotNull Optional<String> destinationDataset,
            @NotNull Optional<String> destinationTable,
            @NotNull Optional<Boolean> deleteResultTable) {
        String outputId = randomAlphaNumStringOfLength(32);
        String destinationDatasetStr = destinationDataset.isPresent() ?
                destinationDataset.get()
                : this.datasetName;
        String destinationTableStr = destinationTable.isPresent() ?
                destinationTable.get()
                : "variant_query_" + outputId;
        TableId destinationTableId = TableId.of(destinationDatasetStr, destinationTableStr);
        QueryJobConfiguration queryJobConfiguration = this.variantQueryToQueryJobConfiguration(
                variantQuery,
                Optional.of(destinationTableId));

        JobInfo jobInfo = JobInfo.of(queryJobConfiguration);
        Job job = bigquery.create(jobInfo);

        log.debug("Entering wait for job " + job.getJobId());
        try {
            job = job.waitFor(this.retryOptions);
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for job " + jobInfo.getJobId());
            throw new RuntimeException(e);
        }
        if (job == null) {
            log.error("Job " + jobInfo.getJobId() + " was null after waitFor");
            throw new RuntimeException("Job " + jobInfo.getJobId() + " does not exist!");
        } else if (job.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(job.getStatus().getError().toString());
        }
        log.info("Finished query job " + job.getJobId());

        // delete table unless told not to
        if (deleteResultTable.isPresent() && !deleteResultTable.get()) {
            log.info("Not deleting result table: " + destinationTableId.toString());
        } else {
            deleteTable(destinationDatasetStr, destinationTableStr);
        }

        // export to GCS
        Table destTable = bigquery.getTable(destinationTableId);
        S3PathParser parser = new S3PathParser("gs://", getOutputLocation());
        String exportDirectory = pathJoin(parser.objectPath, outputId);
        String exportPathFormat = pathJoin(exportDirectory, job.getJobId().getJob() + "-*.csv.gz");
        log.info("Exporting variant query results to GCS location: "
                + "gs://" + pathJoin(parser.bucket, exportPathFormat));
        boolean exported = exportTableToGCS(
                destinationDatasetStr,
                destinationTableStr,
                parser.bucket,
                exportPathFormat
        );
        if (!exported) {
            throw new RuntimeException("Failed to export table");
        }

        // return a BlobId for the directory in GCS where the result files are
        BlobId blobId = BlobId.of(parser.bucket, exportDirectory);
        return blobId;
    }

    public String getStorageBucket() {
        return "gs://krferrit-genome-queries-us-central1";
    }

    public String getOutputLocation() {
        return pathJoin(getStorageBucket(), "bigquery-results/");
        //return "gs://krferrit-genome-queries-us-central1/bigquery-exports/";
    }

    public boolean exportTableToGCS(
            String datasetName,
            String tableName,
            String bucketName,
            String fileNameFormat) {
        Table table = this.bigquery.getTable(datasetName, tableName);
        String gcsUrl = String.format(
                "gs://%s/%s", bucketName, fileNameFormat);
        //String gcsUrl = "gs://my_bucket/filename.csv";
        ExtractJobConfiguration extractJobConfiguration = ExtractJobConfiguration.newBuilder(
                table.getTableId(), gcsUrl)
                .setCompression("GZIP")
                .setFormat("CSV")
                .build();
        JobInfo jobInfo = JobInfo.newBuilder(extractJobConfiguration).build();
        Job job = bigquery.create(jobInfo);

        // Wait for the job to complete
        try {
            Job completedJob = job.waitFor(
                    RetryOption.initialRetryDelay(Duration.ofSeconds(BIGQUERY_INTERVAL_TABLE_EXPORT)),
                    RetryOption.totalTimeout(Duration.ofMinutes(BIGQUERY_TIMEOUT_TABLE_EXPORT)));
            if (completedJob != null && completedJob.getStatus().getError() == null) {
                log.info(String.format(
                        "Finished exporting table %s to GCS location %s",
                        tableName, gcsUrl));
                return true;
            } else {
                log.error("Export job " + jobInfo.getJobId() + " failed!");
                String message = "";
                try {
                    message = completedJob.getStatus().getError().getMessage();
                } catch (NullPointerException ignored) {}
                log.error("Failed to export bigquery table to GCS bucket: " + message);
                return false;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException("Waiting for Table export to GCS job was interrupted");
        }
    }


    @Override
    public long executeCount(CountQuery countQuery, String tableName) {
        //String sql = countQuery.toBigQuerySql(this.projectName, this.datasetName, tableName);
        QueryJobConfiguration queryConfig = countQuery.toBigQueryJobConfig(this.projectName, this.datasetName, tableName);
        TableResult tr;
        try {
            //tr = runSimpleQuery(sql);
            tr = runQueryJob(queryConfig);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        FieldValueList fvl = tr.iterateAll().iterator().next();
        return fvl.get("ct").getLongValue();
    }



    @Override
    public long executeCount(String tableName, String fieldName, Object fieldValue) {
        String query = String.format(
                "select count(*) as ct"
                + " from `%s.%s.%s`",
                this.projectName,
                this.datasetName,
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
        TableResult tr;
        try {
            tr = runSimpleQuery(query);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        FieldValueList fvl = tr.iterateAll().iterator().next();
        return fvl.get("ct").getLongValue();
    }

    public void serializeTableToJson(String tableName, JsonWriter jsonWriter, boolean writeData) throws IOException, InterruptedException {
        String query = String.format("select * from \"%s\".\"%s\"", this.datasetName, tableName);
        TableResult tr = this.runSimpleQuery(query);
        Schema schema = tr.getSchema();
        FieldList fieldList = schema.getFields();
        List<String> columnNames = new ArrayList<>();
        for (Field f : fieldList) {
            columnNames.add(f.getName());
        }
        Long totalRows = tr.getTotalRows();

        // Numeric types in bigquery
        // StandardSQLTypeName.FLOAT64
        // StandardSQLTypeName.INT64
        // StandardSQLTypeName.NUMERIC

        //jsonWriter.beginObject();
        jsonWriter.name("swarm_database_type").value("athena");
        jsonWriter.name("swarm_database_name").value(this.datasetName);
        jsonWriter.name("swarm_table_name").value(tableName);
        jsonWriter.name("data_count").value(totalRows);

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
        for (FieldValueList fieldValueList : tr.iterateAll()) {
            for (String columnName : columnNames) {
                StandardSQLTypeName columnType = fieldList.get(columnName).getType().getStandardType();
                String valString = fieldValueList.get(columnName).getStringValue();
                if (columnType.equals(StandardSQLTypeName.INT64)) {
                    jsonWriter.value(StringUtils.toLongNullable(valString));
                } else if (columnType.equals(StandardSQLTypeName.FLOAT64)) {
                    jsonWriter.value(StringUtils.toDoubleNullable(valString));
                } else if (columnType.equals(StandardSQLTypeName.NUMERIC)) {
                    jsonWriter.value(StringUtils.toDoubleNullable(valString));
                } else {
                    jsonWriter.value(valString);
                }
            }
        }
        jsonWriter.endArray();
        //jsonWriter.endObject();
    }

    @Override
    public Iterable<Map<String,Object>> executeQuery(String query) {
        TableResult tr;
        try {
            tr = runSimpleQuery(query);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        //BigQuery.TableOption to = BigQuery.TableOption();
        //BigQuery.TableOption.fields(BigQuery.TableField.valueOf("abc"));
        //TableId tid = TableId.of(datasetName, tableName);
        //Table t = bigquery.getTable(tid);
        ArrayList<Map<String,Object>> results = new ArrayList<>();
        Schema schema = tr.getSchema();
        FieldList fields = schema.getFields();
        ArrayList<String> fieldNames = new ArrayList<>();
        Iterator<Field> fieldIterator = fields.iterator();
        while (fieldIterator.hasNext()) {
            fieldNames.add(fieldIterator.next().getName());
        }


        Iterable<FieldValueList> it = tr.iterateAll();
        // loop over rows
        for (FieldValueList fvl : it) {
            Map<String,Object> row = new ArrayMap<>();
            //Iterator<FieldValue> fieldIterator = fvl.iterator();
            // within a row, loop over fields
            //Iterator<Field> fieldIterator = fields.iterator();
            //while (fieldIterator.hasNext()) {
            for (String fieldName : fieldNames) {
                //Field curField = fieldIterator.next();
                //String fieldName = curField.getName();
                Object fieldVal = fvl.get(fieldName).getValue();
                //System.out.printf("Field: %s, Value: %s\n", fieldName, fieldVal);
                row.put(fieldName, fieldVal);
            }
            results.add(row);
        }

        return results;
    }

    public TableResult runQueryJob(QueryJobConfiguration queryJobConfig) throws InterruptedException {
        return bigquery.query(queryJobConfig);
    }

    // cache temp table names
    private Map<TableResult,String> tableResultTableMap = new HashMap<>();

    public TableResult runSimpleQueryNoDestination(String sql) throws InterruptedException {
        return runSimpleQuery(sql, Optional.empty());
    }

    public TableResult runSimpleQuery(String sql) throws InterruptedException {
        String tempTableName = "temp_table_" + randomAlphaNumStringOfLength(32);
        TableId destination = TableId.of(this.datasetName, tempTableName);
        return runSimpleQuery(sql, Optional.of(destination));
    }

    public TableResult runSimpleQuery(String sql, @NotNull Optional<TableId> destination) throws InterruptedException {
        log.debug("Running simple query:\n" + sql);
        QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder(sql);

        TableId destId = null;
        if (destination.isPresent()) {
            destId = destination.get();
            builder.setDestinationTable(destId);
            builder.setAllowLargeResults(true); // requires setting a destination table
        }
        QueryJobConfiguration queryConfig = builder.build();
        TableResult tr = runQueryJob(queryConfig);
        if (destination.isPresent()) {
            destId = destination.get();
            String tableName = String.format(
                    "%s.%s.%s", destId.getProject(), destId.getDataset(), destId.getTable()
            );
            tableResultTableMap.put(tr, tableName);
        }
        return tr;
    }

    public void deleteTableFromTableResult(TableResult tr) {
        String tableName = tableResultTableMap.get(tr);
        if (tableName != null) {
            log.debug("Deleting table " + tableName + " from table result");
            deleteTable(tableName);
        } else {
            log.warn("No table name found from table result");
        }
    }

    public void deleteTable(String tableName) {
        deleteTable(this.datasetName, tableName);
    }

    private void deleteTable(String datasetName, String tableName) {
        boolean deleted = bigquery.delete(TableId.of(datasetName, tableName));
        if (!deleted) {
            throw new RuntimeException("Failed to delete table: " + datasetName + "." + tableName);
        }
    }



    public Table createAnnotationTableFromGcs(String tableName, String gcsDirectoryUrl) throws IOException {
        String headersPath = "sql/annotation_headers.csv";
        byte[] bytes = Files.readAllBytes(Paths.get("sql/annotation_headers.sql"));
        String headersString = new String(bytes, StandardCharsets.US_ASCII);
        String[] headers = headersString.split(",");
        assert(headers.length == 186); // this should be removed or changed to the expected count
        ArrayList<Field> fields = new ArrayList<>();
        for (String header : headers) {
            fields.add(Field.newBuilder(header, StandardSQLTypeName.STRING).build());
        }
        Schema schema = Schema.of(fields);
        FormatOptions formatOptions = FormatOptions.csv();
        ExternalTableDefinition tableDefinition =
                ExternalTableDefinition.newBuilder(gcsDirectoryUrl, schema, formatOptions)
                .build();
        TableId tableId = TableId.of(this.datasetName, tableName);
        log.info("Creating table " + tableName + " from gcs directory: " + gcsDirectoryUrl);
        Table newTable = bigquery.create(TableInfo.newBuilder(tableId, tableDefinition).build());
        log.info("Finished creating table " + tableName);
        return newTable;
    }

    public Table createStringTableFromGcs(
            String tableName,
            String gcsDirectoryUrl,
            List<String> columnNames) {
        ArrayList<Field> fields = new ArrayList<>();
        for (String header : columnNames) {
            fields.add(Field.newBuilder(header, StandardSQLTypeName.STRING).build());
        }
        Schema schema = Schema.of(fields);
        FormatOptions formatOptions = FormatOptions.csv();
        ExternalTableDefinition tableDefinition =
                ExternalTableDefinition.newBuilder(gcsDirectoryUrl, schema, formatOptions)
                        .build();
        TableId tableId = TableId.of(this.datasetName, tableName);
        log.info("Creating table " + tableName + " from gcs directory: " + gcsDirectoryUrl);
        Table newTable = bigquery.create(TableInfo.newBuilder(tableId, tableDefinition).build());
        log.info("Finished creating table " + tableName);
        return newTable;
    }

    public Table createVariantTableFromGcs(String tableName, String gcsDirectoryUrl) {
        gcsDirectoryUrl = ensureTrailingSlash(gcsDirectoryUrl);
        log.debug("createVariantTableFromGcs(" + tableName + ", " + gcsDirectoryUrl + ")");
        List<String> sourceUris = new ArrayList<>();
        sourceUris.add(gcsDirectoryUrl);

        Schema schema = Schema.of(
                Field.newBuilder("reference_name", StandardSQLTypeName.STRING).build(),
                Field.newBuilder("start_position", StandardSQLTypeName.INT64).build(),
                Field.newBuilder("end_position", StandardSQLTypeName.INT64).build(),
                Field.newBuilder("reference_bases", StandardSQLTypeName.STRING).build(),
                Field.newBuilder("alternate_bases", StandardSQLTypeName.STRING).build(),
                Field.newBuilder("minor_af", StandardSQLTypeName.FLOAT64).build(),
                Field.newBuilder("allele_count", StandardSQLTypeName.INT64).build()
        );
        FormatOptions formatOptions = FormatOptions.csv();
        ExternalTableDefinition.Builder tableDefinitionBuilder =
                ExternalTableDefinition.newBuilder(gcsDirectoryUrl, schema, formatOptions);
        ExternalTableDefinition tableDefinition = tableDefinitionBuilder.build();
        TableId tableId = TableId.of(this.datasetName, tableName);
        log.info("Creating table " + tableName + " from gcs directory: " + gcsDirectoryUrl);
        Table newTable = bigquery.create(TableInfo.newBuilder(tableId, tableDefinition).build());
        log.info("Finished creating table " + tableName);
        return newTable;
    }

    public long executeCount(VariantQuery variantQuery) throws InterruptedException {
        if (!variantQuery.getCountOnly()) {
            throw new IllegalArgumentException("VariantQuery did not have countOnly set");
        }
        String tablename = "count_query_" + randomAlphaNumStringOfLength(8);
        TableId tableId = TableId.of(this.datasetName, tablename);
        QueryJobConfiguration jobConfiguration =
                this.variantQueryToQueryJobConfiguration(
                        variantQuery,
                        Optional.of(tableId));
        TableResult tr = this.runQueryJob(jobConfiguration);
        return Long.parseLong(
                tr.iterateAll()
                        .iterator()
                        .next()
                        .get("ct")
                        .getStringValue());
    }

    public QueryJobConfiguration variantQueryToQueryJobConfiguration(
            @NotNull VariantQuery variantQuery,
            @NotNull Optional<TableId> destinationTableId) {
        StringBuilder sb = new StringBuilder(String.format(
                "select %s from `%s`",
                variantQuery.getCountOnly() ? "count(*) as ct" : "*",
                this.datasetName + "." + variantQuery.getTableIdentifier()
        ));

        QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder("");

        List<String> wheres = new ArrayList<>();
        if (variantQuery.getReferenceName() != null) {
            wheres.add("reference_name = @referenceName");
            builder.addNamedParameter("referenceName",
                    QueryParameterValue.string(variantQuery.getReferenceName()));
        }

        // position parameters
        if (variantQuery.getUsePositionAsRange()) {
            // range based is a special case
            // TODO convert to prepared statement format
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
                wheres.add("start_position " + variantQuery.getStartPositionOperator() + " @startPosition");
                builder.addNamedParameter("startPosition",
                        QueryParameterValue.int64(variantQuery.getStartPosition()));
            }
            if (variantQuery.getEndPosition() != null) {
                wheres.add("end_position " + variantQuery.getEndPositionOperator() + " @endPosition");
                builder.addNamedParameter("endPosition",
                        QueryParameterValue.int64(variantQuery.getEndPosition()));
            }
        }


        if (variantQuery.getReferenceBases() != null) {
            wheres.add("reference_bases = @referenceBases");
            builder.addNamedParameter("referenceBases",
                    QueryParameterValue.string(variantQuery.getReferenceBases()));
        }
        if (variantQuery.getAlternateBases() != null) {
            wheres.add("alternate_bases = @alternateBases");
            builder.addNamedParameter("alternateBases",
                    QueryParameterValue.string(variantQuery.getAlternateBases()));
        }

        if (variantQuery.getRsid() != null) {
            wheres.add("id = @rsid");
            builder.addNamedParameter("rsid", QueryParameterValue.string(variantQuery.getRsid()));
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
        log.debug("Query: " + queryString);
        builder.setQuery(queryString);
        if (destinationTableId.isPresent()) {
            builder.setDestinationTable(destinationTableId.get());
        }
        builder.setAllowLargeResults(true);
        QueryJobConfiguration queryJobConfiguration = builder.build();
        log.info("Constructed new query job configuration from variant query");
        return queryJobConfiguration;
    }
}
