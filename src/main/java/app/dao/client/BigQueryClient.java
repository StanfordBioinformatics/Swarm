package app.dao.client;

import app.AppLogging;
import app.dao.query.Coordinate;
import app.dao.query.CountQuery;
import app.dao.query.VariantQuery;
import com.google.api.client.util.ArrayMap;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

    // Any columns not in this list are assumed to be sample columns
    private final String[] vcfColumns = new String[]{
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
    private final List<String> vcfColumnsList = Arrays.asList(vcfColumns);

    final static List<String> VCF_JOIN_COLUMNS = Arrays.asList(
            "reference_name",
            "start_position",
            "end_position",
            "reference_bases",
            "alternate_bases");

    public BigQueryClient(
            String datasetName,
            String credentialFilePath) {
        BigQueryOptions.Builder bqb = BigQueryOptions.newBuilder();
        try {
            ServiceAccountCredentials creds = ServiceAccountCredentials.fromStream(new FileInputStream(credentialFilePath));
            bqb.setCredentials(creds);
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

    public BigQuery getBigQuery() {
        return this.bigquery;
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
        variantQuery.setUsePositionAsRange(true);
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

        JobStatistics jobStatistics = job.getStatistics();
        // Get the query-specific statistics
        JobStatistics.QueryStatistics queryStatistics = (JobStatistics.QueryStatistics) jobStatistics;
        log.info("BigQuery bytes scanned: " + queryStatistics.getTotalBytesProcessed());
        log.info("BigQuery execution time: " + (double)(queryStatistics.getEndTime() - queryStatistics.getStartTime())/1000);

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
        return BlobId.of(parser.bucket, exportDirectory);
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

    public List<String> getTableColumns(String tableName) {
        Table table = bigquery.getTable(TableId.of(datasetName, tableName));
        Schema schema = table.getDefinition().getSchema();
        return schema.getFields()
                .stream()
                .map(Field::getName)
                .collect(Collectors.toList());
    }

    /**
     * Merges the provided schemas and removes duplicated column names
     * @param schemaA schema A to merge with B
     * @param tableAliasA alias to use for table A
     * @param schemaB schema B to merge with A
     * @param tableAliasB alias to use for table B
     * @return list of the merged column names, no duplicates
     */
    private List<String> mergeSchemaColumns(
            Schema schemaA, String tableAliasA,
            Schema schemaB, String tableAliasB) {
        String tablePrefixA = tableAliasA + ".";
        String tablePrefixB = tableAliasB + ".";

        List<String> listA = schemaA.getFields()
                .stream()
                .map(Field::getName)
                .map(String::toLowerCase)
                .map(name -> tablePrefixA + name)
                .collect(Collectors.toList());
        List<String> listB = schemaB.getFields()
                .stream()
                .map(Field::getName)
                .map(String::toLowerCase)
                .map(name -> tablePrefixB + name)
                .collect(Collectors.toList());
        List<String> merged = StringUtils.mergeColumnListsIgnorePrefixes(
                listA, tablePrefixA,
                listB, tablePrefixB,
                true);
        return merged;
    }

    public void serializeVcfTableToJson(String tableName, JsonWriter jsonWriter, boolean writeData)
            throws IOException, InterruptedException {
        List<String> cols = this.getTableColumns(tableName);
        Predicate<String> ignoreColPredicate = s -> s.endsWith("_please_ignore");
        List<String> filteredCols = cols.stream()
                .filter(ignoreColPredicate.negate())
                .collect(Collectors.toList());
        String query = "select " + String.join(", ", filteredCols) +
                " from " + String.format("`%s.%s`", datasetName, tableName);

        //String sql = "select * from `" + this.datasetName + "." + tableName + "`";

        jsonWriter.name("swarm_database_type").value("bigquery");
        jsonWriter.name("swarm_database_name").value(this.datasetName);
        jsonWriter.name("swarm_table_name").value(tableName);

        serializeVcfQueryToJson(query, jsonWriter, writeData);
    }

//    public void mergeAndSerializeVcfTablesToJson(String tableName1, String tableName2, JsonWriter jsonWriter)
//            throws IOException, InterruptedException {
//        mergeAndSerializeVcfTablesToJson(tableName1, "a", tableName2, "b", jsonWriter);
//    }

    public String getMergedVcfSelect(
            String tableName1, String table1Alias,
            String tableName2, String table2Alias) {
        return getMergedVcfSelect(tableName1, table1Alias,
                tableName2, table2Alias,
                JoinType.FULL_JOIN);
    }

    public String getMergedVcfSelect(
            String tableName1, String table1Alias,
            String tableName2, String table2Alias,
            JoinType joinType) {
        // merge the schemas, removing duplicated column names
        Table table1 = this.bigquery.getTable(TableId.of(this.datasetName, tableName1));
        Schema schema1 = table1.getDefinition().getSchema();
        Table table2 = this.bigquery.getTable(TableId.of(this.datasetName, tableName2));
        Schema schema2 = table2.getDefinition().getSchema();
        // merge the schemas, removing duplicated column names
        List<String> mergedColumnsNames = mergeSchemaColumns(schema1, table1Alias, schema2, table2Alias);
        String sql =
                "select @mergedColumnsNames " +
                        "from @dataset.@tableName1 as @table1Alias " +
                        joinType.getDefaultString() +
                        " @dataset.@tableName2 as @table2Alias";
        sql = sql.replace("@mergedColumnsNames", String.join(", ", mergedColumnsNames));
        sql = sql.replace("@dataset", this.datasetName);
        sql = sql.replace("@dataset", this.datasetName);
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


//    public void mergeAndSerializeVcfTablesToJson(
//            String tableName1, String table1Alias,
//            String tableName2, String table2Alias,
//            JsonWriter jsonWriter) throws IOException, InterruptedException {
//        String sql = getMergedVcfSelect(tableName1, table1Alias, tableName2, table2Alias);
//        serializeVcfQueryToJson(sql, jsonWriter);
//    }

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
            throws IOException, InterruptedException {
        serializeVcfQueryToJson(query, jsonWriter, true);
    }

    public void serializeVcfQueryToJson(String query, JsonWriter jsonWriter, boolean writeData)
            throws InterruptedException, IOException {
        TableResult tr = this.runSimpleQuery(query);
        Schema schema = tr.getSchema();
        FieldList fieldList = schema.getFields();
        List<String> columnNames = new ArrayList<>();
        Predicate<String> ignoreColPredicate = s -> !s.endsWith("_please_ignore");
//        List<String> filteredCols = columnNames.stream()
//                .filter(ignoreColPredicate.negate())
//                .collect(Collectors.toList());
        for (Field f : fieldList) {
            if (ignoreColPredicate.test(f.getName())) {
                columnNames.add(f.getName());
            }

        }
        Long totalRows = tr.getTotalRows();

        // Sample columns are anything not included in VCF column names
        List<String> sampleColumnsList = new ArrayList<>();
        for (String s : columnNames) {
            if (!StringUtils.listContainsIgnoreCase(vcfColumnsList, s)) {
                sampleColumnsList.add(s);
            }
        }
        //log.debug("Sample column headers: " + Arrays.toString(sampleColumnsList.toArray()));
        List<String> columnsToWrite = Arrays.asList(
                "reference_name", "start_position", "end_position", "id",
                "reference_bases", "alternate_bases", "allele_count", "af");

        jsonWriter.name("data_count").value(totalRows);

        jsonWriter.name("headers").beginArray();
        for (String columnName : columnsToWrite) {
            jsonWriter.value(columnName);
        }
        jsonWriter.endArray();

        if (!writeData) {
            jsonWriter.name("message").value("To return data in response, set return_results query parameter to true");
            return;
        }

        jsonWriter.name("data").beginArray();

        for (FieldValueList fieldValueList : tr.iterateAll()) {
            String[] alternateBases = fieldValueList.get("alternate_bases").getStringValue().split(",");
            int altCount = alternateBases.length;

            int[] alleleCounts = new int[altCount + 1];
            Arrays.fill(alleleCounts, 0);

            // process sample columns, just obtain the allele count and allele frequency for each alt
            // starts at 1 because 0 is the reference allele
            for (String sampleColName : sampleColumnsList) {
                String sampleValue = fieldValueList.get(sampleColName).getStringValue();
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
//            System.out.println(String.format(
//                    "reference_bases: %s, allele_count: %d",
//                    fieldValueList.get("reference_bases").getStringValue(), alleleCounts[0]));
//            for (int i = 0; i < alternateBases.length; i++) {
//                System.out.println(String.format(
//                        "alternate_bases: %s, allele_count: %d",
//                        alternateBases[i], alleleCounts[i+1]));
//            }

            // Write out the data, one row per each alternate bases
            for (int i = 0; i < alternateBases.length; i++) {
                jsonWriter.beginArray();
                // do vcf columns in order
                jsonWriter.value(fieldValueList.get("reference_name").getStringValue());
                jsonWriter.value(fieldValueList.get("start_position").getLongValue());
                jsonWriter.value(fieldValueList.get("end_position").getLongValue());
                jsonWriter.value(fieldValueList.get("id").getStringValue());
                jsonWriter.value(fieldValueList.get("reference_bases").getStringValue());
                jsonWriter.value(alternateBases[i]);
                jsonWriter.value(alleleCounts[i+1]);
                double af = (double)alleleCounts[i+1] / (double)sampleColumnsList.size();
                jsonWriter.value(af);
                jsonWriter.endArray();
            }

        }


        jsonWriter.endArray();
    }

    public void serializeTableToJson(String tableName, JsonWriter jsonWriter, boolean writeData)
            throws IOException, InterruptedException {
        String query = String.format("select * from `%s.%s`", this.datasetName, tableName);
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
        jsonWriter.name("swarm_database_type").value("bigquery");
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
            jsonWriter.beginArray();
            for (String columnName : columnNames) {
                StandardSQLTypeName columnType = fieldList.get(columnName).getType().getStandardType();
                FieldValue fieldValue = fieldValueList.get(columnName);
                if (fieldValue.isNull()) {
                    String s = null;
                    jsonWriter.value(s);
                } else if (columnType.equals(StandardSQLTypeName.INT64)) {
                    jsonWriter.value(fieldValue.getLongValue());
                } else if (columnType.equals(StandardSQLTypeName.FLOAT64)) {
                    jsonWriter.value(fieldValue.getDoubleValue());
                } else if (columnType.equals(StandardSQLTypeName.NUMERIC)) {
                    jsonWriter.value(fieldValue.getNumericValue());
                } else {
                    jsonWriter.value(fieldValue.getStringValue());
                }
            }
            jsonWriter.endArray();
        }
        jsonWriter.endArray();
    }

    /**
     * Not recommended, will result in very large local memory requirements for large result sets
     * @param query query to run
     * @return Iterable of rows, each row being a map of column names to values
     */
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
            for (String fieldName : fieldNames) {
                Object fieldVal = fvl.get(fieldName).getValue();
                //System.out.printf("Field: %s, Value: %s\n", fieldName, fieldVal);
                row.put(fieldName, fieldVal);
            }
            results.add(row);
        }

        return results;
    }

    public TableResult runQueryJob(QueryJobConfiguration queryJobConfig) throws InterruptedException {
        JobId jobId = JobId.newBuilder().setRandomJob().build();
        TableResult tr =  bigquery.query(
                queryJobConfig,
                jobId
        );

        Job job = bigquery.getJob(jobId);
                //BigQuery.JobOption.fields(BigQuery.JobField.values())

        //com.google.cloud.Service<BigQueryOptions> service = (com.google.cloud.Service<BigQueryOptions>) bigquery;
        JobStatistics jobStatistics = job.getStatistics();
        // Get the query-specific statistics
        JobStatistics.QueryStatistics queryStatistics = (JobStatistics.QueryStatistics) jobStatistics;

        log.info("BigQuery bytes scanned: " + queryStatistics.getTotalBytesProcessed());
        log.info("BigQuery execution time: " + (double)(queryStatistics.getEndTime() - queryStatistics.getStartTime())/1000);
        return tr;
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
        builder.setUseQueryCache(true);
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

    public Table createVariantTableFromGcs(String tableName, List<String> sourceUris, List<String> sourceFieldNames) {
        //gcsDirectoryUrl = ensureTrailingSlash(gcsDirectoryUrl);
        log.debug("createVariantTableFromGcs(" + tableName + ", " + String.join(",", sourceUris) + ")");
//        List<String> sourceUris = new ArrayList<>();
//        sourceUris.add(gcsDirectoryUrl);

        // TODO using parquet instead of CSV, schema is built into file
//        List<Field> fields = new ArrayList<>();
//        List<String> allowedIntegerColumns = Arrays.asList("start_position", "end_position");
//        for (String s : sourceFieldNames) {
//            if (allowedIntegerColumns.contains(s)) {
//                fields.add(Field.newBuilder(s, StandardSQLTypeName.INT64).build());
//            } else {
//                fields.add(Field.newBuilder(s, StandardSQLTypeName.STRING).build());
//            }
//        }
//        Schema schema = Schema.of(fields);


//        Schema schema = Schema.of(
//                Field.newBuilder("reference_name", StandardSQLTypeName.STRING).build(),
//                Field.newBuilder("start_position", StandardSQLTypeName.INT64).build(),
//                Field.newBuilder("end_position", StandardSQLTypeName.INT64).build(),
//                Field.newBuilder("reference_bases", StandardSQLTypeName.STRING).build(),
//                Field.newBuilder("alternate_bases", StandardSQLTypeName.STRING).build(),
//                Field.newBuilder("minor_af", StandardSQLTypeName.FLOAT64).build(),
//                Field.newBuilder("allele_count", StandardSQLTypeName.INT64).build()
//        );
//        ExternalTableDefinition.Builder tableDefinitionBuilder =
//                ExternalTableDefinition.newBuilder(sourceUris, schema, formatOptions);


        //FormatOptions formatOptions = FormatOptions.csv();
        FormatOptions formatOptions = FormatOptions.parquet();
        ExternalTableDefinition.Builder tableDefinitionBuilder =
                ExternalTableDefinition.newBuilder(sourceUris.get(0), formatOptions);
        tableDefinitionBuilder.setSourceUris(sourceUris);

        ExternalTableDefinition tableDefinition = tableDefinitionBuilder.build();
        TableId tableId = TableId.of(this.datasetName, tableName);
        log.info("Creating table " + tableName + " from gcs uris: " + String.join(",", sourceUris));
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

        StringBuilder sb = new StringBuilder("select");

        if (variantQuery.getCountOnly()) {
            sb.append(" count(*) as ct");
        } else {
            if (variantQuery.isVariantColumnsOnly()) {
                sb.append(" ").append(StringUtils.join(vcfColumns, ", "));
            } else {
                List<String> colNames = this.getTableColumns(variantQuery.getTableIdentifier());
                sb.append(" ").append(StringUtils.join(colNames, ", "));
            }
        }
        sb.append(String.format(" from `%s`",
                this.datasetName + "." + variantQuery.getTableIdentifier()));

//        StringBuilder sb = new StringBuilder(String.format(
//                "select %s from `%s`",
//                variantQuery.getCountOnly() ? "count(*) as ct" : "*",
//                this.datasetName + "." + variantQuery.getTableIdentifier()
//        ));

        QueryJobConfiguration.Builder builder = QueryJobConfiguration.newBuilder(sb.toString());

        List<String> wheres = new ArrayList<>();
        if (variantQuery.getReferenceName() != null) {
            wheres.add("reference_name = @referenceName");
            builder.addNamedParameter("referenceName",
                    QueryParameterValue.string(variantQuery.getReferenceName()));
        }

        // position parameters
        if (variantQuery.getUsePositionAsRange()) {
            // range based is a special case
//            String whereTerm =
//                    " ((start_position >= %s and start_position <= %s)" // start pos overlaps gene
//                            + " or (end_position >= %s and end_position <= %s)" // end pos overlaps gene
//                            + " or (start_position < %s and end_position > %s))"; // interval fully contains a gene
            String whereTerm = "(start_position <= %s and start_position >= %s)";
            String start = variantQuery.getStartPosition() != null ?
                    variantQuery.getStartPosition().toString()
                    : "0";
            String end = variantQuery.getEndPosition() != null ?
                    variantQuery.getEndPosition().toString()
                    : Long.valueOf(Long.MAX_VALUE).toString();
            whereTerm = String.format(whereTerm, end, start);
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
