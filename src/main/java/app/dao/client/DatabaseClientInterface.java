package app.dao.client;

import app.dao.query.CountQuery;

import java.util.Map;

public interface DatabaseClientInterface {

    enum JoinType {
        LEFT_JOIN("left join"),
        RIGHT_JOIN("right join"),
        FULL_JOIN("full join"),
        INNER_JOIN("inner join"),
        CROSS_JOIN("cross join");

        private String defaultString;

        JoinType(String defaultString) {
            this.defaultString = defaultString;
        }

        String getDefaultString() {
            return defaultString;
        }
    }

    /**
     * Warning, highly susceptible to out of memory errors, as it loads result set into local memory.
     * @param query
     * @return
     */
    Iterable<Map<String,Object>> executeQuery(String query);

    /**
     * Count the rows of tableName where column `fieldName` = value `fieldValue`
     * @param tableName
     * @param fieldName
     * @param fieldValue
     * @return
     */
    long executeCount(String tableName, String fieldName, Object fieldValue);

    /**
     * Count the rows of tableName matching the countQuery criteria
     * @param countQuery
     * @param tableName
     * @return
     */
    long executeCount(CountQuery countQuery, String tableName);

}
