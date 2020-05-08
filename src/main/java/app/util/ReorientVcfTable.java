package app.util;

import app.dao.client.AthenaClient;
import app.dao.client.BigQueryClient;
import com.google.api.services.bigquery.model.Row;
import com.google.api.services.bigquery.model.Table;
import com.google.cloud.bigquery.*;
import com.simba.athena.jdbc.utils.ParseQueryUtils;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class ReorientVcfTable {

    BigQueryClient bigQueryClient;
    TableId tableId;

    public ReorientVcfTable(BigQueryClient bigQueryClient, String tableName) {
        this.bigQueryClient = bigQueryClient;
        this.tableId = TableId.of(bigQueryClient.getProjectName(), bigQueryClient.getDatasetName(), tableName);
    }

    void printErrors(InsertAllResponse insertAllResponse) {
        insertAllResponse.getInsertErrors().forEach((id, errors) -> {
            System.err.printf("%d: %s\n", id, Arrays.toString(errors.toArray()));
        });
    }

    void insertRow(Row row) {
        List<InsertAllRequest.RowToInsert> rows = new ArrayList<>();
        InsertAllRequest insertAllRequest = InsertAllRequest
                .newBuilder(tableId, rows)
                .setIgnoreUnknownValues(false)
                .build();
        InsertAllResponse insertAllResponse = bigQueryClient.getBigQuery().insertAll(insertAllRequest);
        if (insertAllResponse.hasErrors()) {
            printErrors(insertAllResponse);
            throw new RuntimeException("Failed to insert row");
        }
    }

    public void deleteTable() {
        bigQueryClient.deleteTable(tableId.getTable());
    }

    public void createTable() {
        Schema schema = Schema.of(new ArrayList<>());
        TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .build();

        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        this.bigQueryClient.getBigQuery().create(tableInfo); // Can add TableOptions
    }

    void doReorient() throws InterruptedException {
        String sql = String.format("select * from `%s.%s.%s",
                tableId.getProject(),
                tableId.getDataset(),
                tableId.getTable());
        TableResult tr = bigQueryClient.runSimpleQuery(sql);
        Schema schema = tr.getSchema();
        for (FieldValueList fieldValueList : tr.iterateAll()) {
            FieldList fields = schema.getFields();
            //Iterator<Field> fieldIterator = fields.iterator();
            for (Field f : fields) {

            }
        }
    }

    public static void main(String[] args) throws IOException {
        String gcpCredentialFilePath = "gcp.json";
        String awsCredentialFilePath = "aws.properties";
        BigQueryClient bigQueryClient = new BigQueryClient("swarm", gcpCredentialFilePath);
        AthenaClient athenaClient = new AthenaClient("swarm", awsCredentialFilePath);
//        ReorientVcfTable reorientVcfTable = new ReorientVcfTable(bigQueryClient, );
//        reorientVcfTable.doReorient('');

//        ParquetReader pr = ParquetReader.builder();

        ParquetFileReader reader = ParquetFileReader.open(new InputFile() {
            @Override
            public long getLength() throws IOException {
                return 0;
            }

            @Override
            public SeekableInputStream newStream() throws IOException {
                return null;
            }
        });
    }
}
