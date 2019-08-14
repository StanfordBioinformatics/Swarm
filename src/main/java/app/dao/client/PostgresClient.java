package app.dao.client;

import org.springframework.lang.Nullable;

import java.util.Map;

import java.sql.*;
import java.util.UUID;

public class PostgresClient {

    private final String host;
    private final String database;
    private final int port;
    private final String username;
    private final String password;

    public PostgresClient(String host, String database,
                          @Nullable String username, @Nullable String password) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        this.host = host;
        this.database = database;
        this.port = 5432;
        this.username = username;
        this.password = password;
    }

    private Connection getConnection() throws SQLException {
        String jdbUrl = String.format(
                "jdbc:postgresql://%s:%d/%s",
                this.host, this.port, this.database);
        Connection conn = DriverManager.getConnection(jdbUrl);
        return conn;
    }

    /**
     *
     * @param tableName The name of the table to create
     * @param schema A map of `columnname` to `type`
     * @throws SQLException
     */
    public void createTable(String tableName, Map<String,String> schema) throws SQLException {
        //String tempTableName = UUID.randomUUID().toString();

        StringBuilder sb = new StringBuilder();
        sb.append("create table ").append(tableName);
        sb.append(" (");
        schema.forEach((key, value) -> {
            sb.append(key).append(" ").append(value).append(",");
        });
        if (sb.charAt(sb.length() - 1) == ',') {
            sb.setLength(sb.length() - 1); // remove trailing comma
        }
        sb.append(")");

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sb.toString());
        ){
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw e;
        }
    }
}
