package pl.kognitywistyka;

import java.sql.*;

/**
 * Created by pwilkin on 26.05.2022.
 */
public class DbManager {

    private Connection connection;

    private Connection getConnection() {
        return connection;
    }

    public Connection getValidatedConnection() {
        testConnection();
        return getConnection();
    }

    public void testConnection() {
        try {
            if (connection.isClosed() || !connection.isValid(1000)) {
                obtainConnection();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void obtainConnection() {
        try {
            this.connection = DriverManager.getConnection("jdbc:hsqldb:file:maindb", "SA", "");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean tableExists(Connection connection, String tableName) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet resultSet = meta.getTables(null, null, tableName, new String[] {"TABLE"});
        return resultSet.next();
    }

    private void createTableIfNotExists(Connection c, String table, String definition) throws SQLException {
        if (!tableExists(c, table)) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE " + table + " " + definition);
            }
        }
    }

    public void prepareDbIfNeeded() {
        try {
            Connection c = getValidatedConnection();
            createTableIfNotExists(c, "GOODS", "(ID INT IDENTITY, NAME VARCHAR(255), COST DECIMAL(8, 2), QUANTITY INT)");
            createTableIfNotExists(c, "CUSTOMERS", "(ID INT IDENTITY, FIRST_NAME VARCHAR(255), LAST_NAME VARCHAR(255), EMAIL VARCHAR(255))");
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public DbManager() {
        obtainConnection();
    }

}
