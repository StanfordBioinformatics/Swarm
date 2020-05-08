package app.util;

import app.AppLogging;
import app.dao.client.AthenaClient;
import app.dao.client.BigQueryClient;
import app.dao.client.StringUtils;
import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.amazonaws.services.athena.model.GetQueryResultsResult;
import com.amazonaws.services.athena.model.ResultSet;
import com.amazonaws.services.athena.model.Row;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import org.apache.avro.generic.GenericData;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TableCleaner {
    private Logger log = AppLogging.getLogger(TableCleaner.class);
    private BigQueryClient bigQueryClient;
    private AthenaClient athenaClient;

    private List<Pattern> patterns = Arrays.asList(
            Pattern.compile("genome_query_.*"),
            Pattern.compile("variant_query_.*"),
            Pattern.compile("bigquery_import_.*"),
            Pattern.compile("athena_import_.*"),
            Pattern.compile("swarm_annotation_.*"),
            Pattern.compile("merge_.*")
    );

    public TableCleaner(BigQueryClient bigQueryClient, AthenaClient athenaClient) {
        this.bigQueryClient = bigQueryClient;
        this.athenaClient = athenaClient;
    }

    public boolean matchesTablePatterns(String tableName) {
        for (Pattern p : patterns) {
            if (p.matcher(tableName).matches()) {
                return true;
            }
        }
        return false;
    }

//    public List<String> clearTables() {
//        List<String> deletedBigQueryTableNames = clearBigQueryTables();
//
//
//        return deletedBigQueryTableNames;
//    }

    public List<String> clearBigQueryTables() {
        Page<Table> page = bigQueryClient.getBigQuery().listTables(DatasetId.of(
                bigQueryClient.getDatasetName()));
        List<String> deletedNames = new ArrayList<>();
        int pageCount = 0;
        do {
            pageCount++;
            List<Table> pageTableList = new ArrayList<>();
            page.iterateAll().forEach(pageTableList::add);
            for (Table table : pageTableList) {
                //log.debug("getFriendlyName(): " + table.getFriendlyName());
                //log.debug("getTableId().getTable(): " + table.getTableId().getTable());
                String tableName = table.getTableId().getTable();
                if (matchesTablePatterns(tableName)) {
                    System.out.println(table.getTableId());
                    bigQueryClient.getBigQuery().delete(TableId.of(
                            bigQueryClient.getProjectName(),
                            bigQueryClient.getDatasetName(),
                            tableName
                    ));
                    deletedNames.add(table.getTableId().getTable());
                }
            }
        } while (page.hasNextPage() && (page = page.getNextPage()) != null);
        log.info("Page count: " + pageCount);
        return deletedNames;
    }

    public String getS3LocationForTable(String tableName) {
        String sql = String.format("SHOW CREATE TABLE `%s.%s`",
                this.athenaClient.getDatabaseName(), tableName);
        GetQueryResultsResult getQueryResultsResult = athenaClient.executeQueryToResultSet(sql);
        ResultSet rs = getQueryResultsResult.getResultSet();
        for (Row row : rs.getRows()) {
            System.out.println(row.toString());
        }
        return "";
    }

    public List<String> clearAthenaTables() {
        List<String> tableNames = athenaClient.listTables();
        List<String> deletedTableNames = new ArrayList<>();
        System.out.println("Table locations:");
        for (String tableName : tableNames) {
            if (matchesTablePatterns(tableName)) {
                // TODO location parser not implemented yet
//                String tableLocation = getS3LocationForTable(tableName);
                System.out.printf("%s\n", tableName);
                athenaClient.deleteTable(tableName);

                deletedTableNames.add(tableName);

            }
        }

        return deletedTableNames;
    }

    public static void main(String[] args) {
        String gcpCredentialFilePath = "gcp.json";
        String awsCredentialFilePath = "aws.properties";
        BigQueryClient bigQueryClient = new BigQueryClient("swarm", gcpCredentialFilePath);
        AthenaClient athenaClient = new AthenaClient("swarm", awsCredentialFilePath);
        TableCleaner tableCleaner = new TableCleaner(bigQueryClient, athenaClient);

        List<String> deletedBigQueryTableNames = tableCleaner.clearBigQueryTables();
        System.out.println("DELETED BIGQUERY TABLES");
        for (String s : deletedBigQueryTableNames) {
            System.out.println(s);
        }

        List<String> deletedAthenaTableNames = tableCleaner.clearAthenaTables();
        System.out.println("DELETED ATHENA TABLES\n");
        for (String s : deletedAthenaTableNames) {
            System.out.println(s);
        }
    }
}
