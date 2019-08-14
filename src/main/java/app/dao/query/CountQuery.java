package app.dao.query;

import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;

import java.util.ArrayList;
import java.util.List;

public class CountQuery {
    private String referenceName;
    private Long startPosition;
    private Long endPosition;
    private String referenceBases;
    private String alternateBases;

    private boolean usePositionAsRange = false;

    public QueryJobConfiguration toBigQueryJobConfig(
            String projectName, String datasetName, String tableName
    ) {
        StringBuilder sb = new StringBuilder(String.format(
                "select count(*) as ct from `%s.%s.%s`",
                projectName, datasetName, tableName));

        List<String> wheres = new ArrayList<>();


        QueryJobConfiguration.Builder queryJobConfig = QueryJobConfiguration.newBuilder(sb.toString());

        if (this.referenceName != null) {
            wheres.add("reference_name = ?");
            queryJobConfig.addPositionalParameter(QueryParameterValue.string(referenceName));
        }

        if (this.startPosition != null) {
            wheres.add(String.format(
                    "start_position %s ?",
                    usePositionAsRange ? ">=" : "="
            ));
            queryJobConfig.addPositionalParameter(QueryParameterValue.int64(startPosition));
        }

        if (this.endPosition != null) {
            wheres.add(String.format(
                    "end_position %s ?",
                    usePositionAsRange ? "<=" : "="
            ));
            queryJobConfig.addPositionalParameter(QueryParameterValue.int64(endPosition));
        }

        if (this.referenceBases != null) {
            wheres.add("reference_bases = ?");
            queryJobConfig.addPositionalParameter(QueryParameterValue.string(referenceBases));
        }

        if (this.alternateBases != null) {
            wheres.add("alternate_bases = ?");
            queryJobConfig.addPositionalParameter(QueryParameterValue.string(alternateBases));
        }

        // construct the where clause
        for (int i = 0; i < wheres.size(); i++) {
            if (i == 0) {
                sb.append(" where ");
            }
            if (i > 0) {
                sb.append(" and ");
            }
            sb.append(wheres.get(i));
        }

        queryJobConfig.setQuery(sb.toString());


        return queryJobConfig.build();
    }

    public CountQuery() {}

    public String getReferenceName() {
        return referenceName;
    }

    public void setReferenceName(String referenceName) {
        this.referenceName = referenceName;
    }

    public Long getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(Long startPosition) {
        this.startPosition = startPosition;
    }

    public Long getEndPosition() {
        return endPosition;
    }

    public void setEndPosition(Long endPosition) {
        this.endPosition = endPosition;
    }

    public String getReferenceBases() {
        return referenceBases;
    }

    public void setReferenceBases(String referenceBases) {
        this.referenceBases = referenceBases;
    }

    public String getAlternateBases() {
        return alternateBases;
    }

    public void setAlternateBases(String alternateBases) {
        this.alternateBases = alternateBases;
    }

    public boolean isUsePositionAsRange() {
        return usePositionAsRange;
    }

    public void setUsePositionAsRange(boolean usePositionAsRange) {
        this.usePositionAsRange = usePositionAsRange;
    }
}
