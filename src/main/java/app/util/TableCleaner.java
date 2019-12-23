package app.util;

import app.AppLogging;
import app.dao.client.AthenaClient;
import app.dao.client.BigQueryClient;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Table;
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
            for (Table table : page.iterateAll()) {
                //log.debug("getFriendlyName(): " + table.getFriendlyName());
                //log.debug("getTableId().getTable(): " + table.getTableId().getTable());
                String tableName = table.getTableId().getTable();
                if (matchesTablePatterns(tableName)) {
                    deletedNames.add(table.getTableId().getTable());
                }
            }
        } while (page.hasNextPage() && (page = page.getNextPage()) != null);
        log.info("Page count: " + pageCount);
        return deletedNames;
    }

    public static void main(String[] args) {
        String gcpCredentialFilePath = "gcp.json";
        String awsCredentialFilePath = "aws.properties";
        BigQueryClient bigQueryClient = new BigQueryClient("swarm", gcpCredentialFilePath);
        AthenaClient athenaClient = new AthenaClient("swarm", awsCredentialFilePath);
        TableCleaner tableCleaner = new TableCleaner(bigQueryClient, athenaClient);
        List<String> deletedTableNames = tableCleaner.clearBigQueryTables();

        System.out.println("DELETED BIGQUERY TABLES");
        for (String s : deletedTableNames) {
            System.out.println(s);
        }
    }
}
