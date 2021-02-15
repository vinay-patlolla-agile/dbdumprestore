package com.utility.dbdumprestore;

import com.utility.dbdumprestore.model.DbExportDeleteProperties;
import com.utility.dbdumprestore.model.DbExportInsertProperties;
import com.utility.dbdumprestore.model.DbImportProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Component
public class Utility {

        private static Logger logger = LoggerFactory.getLogger(Utility.class);

        private final DbImportProperties dbImportProperties;
        private final DbExportInsertProperties dbExportInsertProperties;
        private final DbExportDeleteProperties dbExportDeleteProperties;

        public Utility(DbExportInsertProperties dbExportInsertProperties, DbExportDeleteProperties dbExportDeleteProperties, DbImportProperties dbImportProperties){
            this.dbExportInsertProperties= dbExportInsertProperties;
            this.dbExportDeleteProperties= dbExportDeleteProperties;
            this.dbImportProperties = dbImportProperties;
        }

        static final String SQL_START_PATTERN = "-- start";
        static final String SQL_END_PATTERN = "-- end";

        /**
         * This is a utility function for connecting to a
         * database instance that's running on localhost at port 3306.
         * It will build a JDBC URL from the given parameters and use that to
         * obtain a connect from doConnect()
         * @param username database username
         * @param password database password
         * @param database database name
         * @param driverName the user supplied mysql connector driver class name. Can be empty
         * @return Connection
         * @throws ClassNotFoundException exception
         * @throws SQLException exception
         */
        static Connection connect(String username, String password, String database, String driverName) throws ClassNotFoundException, SQLException {
        String url = "jdbc:mysql://localhost:3306/" + database + "?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true";
        String driver = (Objects.isNull(driverName) || driverName.isEmpty()) ? "com.mysql.cj.jdbc.Driver" : driverName;
        return doConnect(driver, url, username, password);
    }

        /**
         * This is a utility function that allows connecting
         * to a database instance identified by the provided jdbcURL
         * The connector driver name can be empty
         * @param username database username
         * @param password database password
         * @param jdbcURL the user supplied JDBC URL. It's used as is. So ensure you supply the right parameters
         * @param driverName the user supplied mysql connector driver class name
         * @return Connection
         * @throws ClassNotFoundException exception
         * @throws SQLException exception
         */
        static Connection connectWithURL(String username, String password, String jdbcURL, String driverName) throws ClassNotFoundException, SQLException {
        String driver = (Objects.isNull(driverName) || driverName.isEmpty()) ? "com.mysql.cj.jdbc.Driver" : driverName;
        return doConnect(driver, jdbcURL, username, password);
    }

        /**
         * This will attempt to connect to a database using
         * the provided parameters.
         * On success it'll return the java.sql.Connection object
         * @param driver the class name for the mysql driver to use
         * @param url the url of the database
         * @param username database username
         * @param password database password
         * @return Connection
         * @throws SQLException exception
         * @throws ClassNotFoundException exception
         */
        private static Connection doConnect(String driver, String url, String username, String password) throws SQLException, ClassNotFoundException {
        Class.forName(driver);
        Connection connection = DriverManager.getConnection(url, username, password);
        logger.debug("DB Connected Successfully");
        return  connection;
    }

    /**
     * This is a utility function to validate and get the table info
     * of the supplied table and  database
     * @param database the database name
     * @param stmt Statement object
     * @return TableResponse object containing the list of tables and views
     * @throws SQLException exception
     */
    static TablesResponse getTables(String database, Statement stmt,String parentTable,String childTable) throws SQLException {

        List<String> tables = new ArrayList<>();
        List<String> views = new ArrayList<>();

        ResultSet rs;
        rs = stmt.executeQuery("SHOW TABLE STATUS FROM `" + database + "`;");
        while ( rs.next() ) {
            String comment = rs.getString("Comment");
            if(! "VIEW".equals(comment) && (parentTable.equalsIgnoreCase(rs.getString("Name")) || childTable.equalsIgnoreCase(rs.getString("Name")))) {
                tables.add(rs.getString("Name"));
            }
        }

        return new TablesResponse(tables, views);
    }

        /**
         * This is a utility function to get the names of all
         * the tables and views that're in the database supplied
         * @param database the database name
         * @param stmt Statement object
         * @return TableResponse object containing the list of tables and views
         * @throws SQLException exception
         */
        static TablesResponse getAllTablesAndViews(String database, Statement stmt) throws SQLException {

        List<String> tables = new ArrayList<>();
        List<String> views = new ArrayList<>();

        ResultSet rs;
        rs = stmt.executeQuery("SHOW TABLE STATUS FROM `" + database + "`;");
        while ( rs.next() ) {
            String comment = rs.getString("Comment");
            if("VIEW".equals(comment)) {
                views.add(rs.getString("Name"));
            }
            else {
                tables.add(rs.getString("Name"));
            }
        }

        return new TablesResponse(tables, views);
    }

        /**
         * This function is an helper function
         * that'll generate a DELETE FROM database.table
         * SQL to clear existing table
         * @param database database
         * @param table  table
         * @return String sql to delete the all records from the table
         */
        static String getEmptyTableSQL(String database, String table) {
        String safeDeleteSQL = "SELECT IF( \n" +
            "(SELECT COUNT(1) as table_exists FROM information_schema.tables \n" +
            "WHERE table_schema='" + database + "' AND table_name='" + table + "') > 1, \n" +
            "'DELETE FROM " + table + "', \n" +
            "'SELECT 1') INTO @DeleteSQL; \n" +
            "PREPARE stmt FROM @DeleteSQL; \n" +
            "EXECUTE stmt; DEALLOCATE PREPARE stmt; \n";

        return  "\n" + Utility.SQL_START_PATTERN + "\n" +
            safeDeleteSQL + "\n" +
            "\n" + Utility.SQL_END_PATTERN + "\n";
    }

    /**
     * This function will check if the required minimum
     * properties are set for database connection and exporting
     * password is excluded here because it's possible to have a mysql database
     * user with no password
     * @return true if all required properties are present and false if otherwise
     */
    public boolean isValidateDbExportInsertProperties() {
        return dbExportInsertProperties != null &&
            StringUtils.hasLength(dbExportInsertProperties.getUsername()) &&
            (StringUtils.hasLength(dbExportInsertProperties.getDbName()) || StringUtils.hasLength(dbExportInsertProperties.getJdbcUrl()));
    }

    /**
     * This function will check if the required minimum
     * properties are set for database connection and exporting
     * password is excluded here because it's possible to have a mysql database
     * user with no password
     * @return true if all required properties are present and false if otherwise
     */
    public boolean isValidateDbExportDeleteProperties() {
        return dbExportDeleteProperties != null &&
            StringUtils.hasLength(dbExportDeleteProperties.getUsername()) &&
            (StringUtils.hasLength(dbExportDeleteProperties.getDbName()) || StringUtils.hasLength(dbExportDeleteProperties.getJdbcUrl()));
    }

    /**
     * This function will check if the required minimum
     * properties are set for database connection and exporting
     * password is excluded here because it's possible to have a mysql database
     * user with no password
     * @return true if all required properties are present and false if otherwise
     */
    public boolean isValidateDbImportProperties() {
        return dbImportProperties != null &&
            StringUtils.hasLength(dbImportProperties.getUsername()) &&
            (StringUtils.hasLength(dbImportProperties.getDbName()) || StringUtils.hasLength(dbImportProperties.getJdbcUrl()));
    }


    public Connection getExportInsertConnection() throws SQLException, ClassNotFoundException {
        String database = dbExportInsertProperties.getDbName();
        String jdbcURL = dbExportInsertProperties.getJdbcUrl();
        String driverName = dbExportInsertProperties.getJdbcDriver();

        Connection connection = null;
        if(jdbcURL.isEmpty()) {
            connection = Utility.connect(dbExportInsertProperties.getUsername(), dbExportInsertProperties.getPassword(),
                database, driverName);
        }else {
            if (jdbcURL.contains("?")) {
                database = jdbcURL.substring(jdbcURL.lastIndexOf("/") + 1, jdbcURL.indexOf("?"));
            } else {
                database = jdbcURL.substring(jdbcURL.lastIndexOf("/") + 1);
            }
            logger.debug("database name extracted from connection string: " + database);
            connection = Utility.connectWithURL(dbExportInsertProperties.getUsername(), dbExportInsertProperties.getPassword(),
                jdbcURL, driverName);
        }
        return connection;
    }

    public Connection getExportDeleteConnection() throws SQLException, ClassNotFoundException {
        String database = dbExportDeleteProperties.getDbName();
        String jdbcURL = dbExportDeleteProperties.getJdbcUrl();
        String driverName = dbExportDeleteProperties.getJdbcDriver();

        Connection connection = null;
        if(jdbcURL.isEmpty()) {
            connection = Utility.connect(dbExportDeleteProperties.getUsername(), dbExportDeleteProperties.getPassword(),
                database, driverName);
        }else {
            if (jdbcURL.contains("?")) {
                database = jdbcURL.substring(jdbcURL.lastIndexOf("/") + 1, jdbcURL.indexOf("?"));
            } else {
                database = jdbcURL.substring(jdbcURL.lastIndexOf("/") + 1);
            }
            logger.debug("database name extracted from connection string: " + database);
            connection = Utility.connectWithURL(dbExportDeleteProperties.getUsername(), dbExportDeleteProperties.getPassword(),
                jdbcURL, driverName);
        }
        return connection;
    }

    public Connection getImportDbConnection() throws SQLException, ClassNotFoundException {
        String database = dbImportProperties.getDbName();
        String jdbcURL = dbImportProperties.getJdbcUrl();
        String driverName = dbImportProperties.getJdbcDriver();

        Connection connection = null;
        if(jdbcURL.isEmpty()) {
            connection = Utility.connect(dbImportProperties.getUsername(), dbImportProperties.getPassword(),
                database, driverName);
        }else {
            if (jdbcURL.contains("?")) {
                database = jdbcURL.substring(jdbcURL.lastIndexOf("/") + 1, jdbcURL.indexOf("?"));
            } else {
                database = jdbcURL.substring(jdbcURL.lastIndexOf("/") + 1);
            }
            logger.debug("database name extracted from connection string: " + database);
            connection = Utility.connectWithURL(dbImportProperties.getUsername(), dbImportProperties.getPassword(),
                jdbcURL, driverName);
        }
        return connection;
    }

}
