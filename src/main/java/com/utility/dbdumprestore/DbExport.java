package com.utility.dbdumprestore;


import com.utility.dbdumprestore.model.DbProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.zeroturnaround.zip.ZipUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

@Component
public class DbExport {

    private DbProperties dbProperties;

    public DbExport(DbProperties dbProperties){
        this.dbProperties = dbProperties;
    }
    private Statement stmt;

    private Connection connection;
    private String database;
    private String generatedSql = "";
    private Logger logger = LoggerFactory.getLogger(getClass());
    private final String LOG_PREFIX = "DB-Export: ";
    private String dirName = "DUMP";
    private String sqlFileName = "";
    private String zipFileName = "";
    //private Properties properties;
    private File generatedZipFile;



    /**
     * @deprecated
     * This is deprecated in favour of the same option available
     * in the {@link Utility} class.
     */
    public static final String DROP_TABLES = "DROP_TABLES";


    /**
     * @deprecated
     * This is deprecated in favour of the same option available
     * in the {@link Utility} class.
     */
    public static final String DELETE_EXISTING_DATA = "DELETE_EXISTING_DATA";


    public static final String JDBC_CONNECTION_STRING = "JDBC_CONNECTION_STRING";
    public static final String JDBC_DRIVER_NAME = "JDBC_DRIVER_NAME";
    public static final String SQL_FILE_NAME = "SQL_FILE_NAME";

    /**
     * This function will check if the required minimum
     * properties are set for database connection and exporting
     * password is excluded here because it's possible to have a mysql database
     * user with no password
     * @return true if all required properties are present and false if otherwise
     */
    private boolean isValidateProperties() {
        return dbProperties != null &&
            StringUtils.hasLength(dbProperties.getUsername()) &&
            (StringUtils.hasLength(dbProperties.getName()) || StringUtils.hasLength(dbProperties.getJdbcUrl()));
    }



    /**
     * This function will return true
     * or false based on the availability
     * or absence of a custom output sql
     * file name
     * @return bool
     */
    private boolean isSqlFileNamePropertySet(){
        return dbProperties != null &&
            StringUtils.hasLength(dbProperties.getSqlFileName());
    }

    /**
     * This will generate the SQL statement
     * for creating the table supplied in the
     * method signature
     * @param table the table concerned
     * @return String
     * @throws SQLException exception
     */
    private String getTableInsertStatement(String table) throws SQLException {

        StringBuilder sql = new StringBuilder();
        ResultSet rs;
        boolean addIfNotExists = dbProperties.getCreateTableIfNotExisits() ? true:false;


        if(table != null && !table.isEmpty()){
            rs = stmt.executeQuery("SHOW CREATE TABLE " + "`" + table + "`;");
            while ( rs.next() ) {
                String qtbl = rs.getString(1);
                String query = rs.getString(2);
                sql.append("\n\n--");
                sql.append("\n").append(Utility.SQL_START_PATTERN).append("  table dump : ").append(qtbl);
                sql.append("\n--\n\n");

                if(addIfNotExists) {
                    query = query.trim().replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
                }

                sql.append(query).append(";\n\n");
            }

            sql.append("\n\n--");
            sql.append("\n").append(Utility.SQL_END_PATTERN).append("  table dump : ").append(table);
            sql.append("\n--\n\n");
        }

        return sql.toString();
    }

    /**
     * this will generate the SQL statement to re-create
     * the supplied view
     * @param view the name of the View
     * @return an SQL to create the view
     * @throws SQLException on error
     */
    private String getCreateViewStatement(String view) throws SQLException {

        StringBuilder sql = new StringBuilder();
        ResultSet rs;

        if(view != null && !view.isEmpty()) {
            rs = stmt.executeQuery("SHOW CREATE VIEW " + "`" + view + "`;");
            while ( rs.next() ) {
                String viewName = rs.getString(1);
                String viewQuery = rs.getString(2);
                sql.append("\n\n--");
                sql.append("\n").append(Utility.SQL_START_PATTERN).append("  view dump : ").append(view);
                sql.append("\n--\n\n");

                String finalQuery = "CREATE OR REPLACE VIEW `" + viewName + "` " + (viewQuery.substring(viewQuery.indexOf("AS")).trim());
                sql.append(finalQuery).append(";\n\n");
            }

            sql.append("\n\n--");
            sql.append("\n").append(Utility.SQL_END_PATTERN).append("  view dump : ").append(view);
            sql.append("\n--\n\n");
        }

        return sql.toString();
    }


    /**
     * This function will generate the insert statements needed
     * to recreate the table under processing.
     * @param parentTable the table to get inserts statement for
     * @return String generated SQL insert
     * @throws SQLException exception
     */
    private String getDataInsertStatement(String parentTable,String childTable,String relatedColumn,String fromCreatedDateTime,String toCreatedDateTime) throws SQLException {

        StringBuilder sql = new StringBuilder();
        ResultSet parentResultSet = null;
        String query = null;

        parentResultSet = getResultSetFromParentTable(parentTable,fromCreatedDateTime,toCreatedDateTime);

        //move to the last row to get max rows returned
        parentResultSet.last();
        int rowCount = parentResultSet.getRow();

        //there are no records in the parent table just return empty string
        if(rowCount <= 0) {
            logger.debug("No records found for the table ",parentTable);
            return sql.toString();
        }

        sql.append("\n--").append("\n-- Inserts of ").append(parentTable).append(" AND ").append(childTable).append("\n--\n\n");
        sql.append("\n--").append("\n-- Inserts of ").append(childTable).append("\n--\n\n");

        //temporarily disable foreign key constraint
        sql.append("\n/*!40000 ALTER TABLE `").append(parentTable).append("` DISABLE KEYS */;\n");
        sql.append("\n/*!40000 ALTER TABLE `").append(childTable).append("` DISABLE KEYS */;\n");

        sql.append("\n--\n")
            .append(Utility.SQL_START_PATTERN).append(" table insert : ").append(parentTable)
            .append("\n--\n");

        sql.append("\n--\n")
            .append(Utility.SQL_START_PATTERN).append(" table insert : ").append(childTable)
            .append("\n--\n");



        ResultSetMetaData metaData = parentResultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        int relatedColumnType = Types.VARCHAR;
        StringBuilder columns = new StringBuilder();

        //generate the column names that are present
        //in the returned result set
        //at this point the insert is INSERT INTO (`col1`, `col2`, ...)
        for(int i = 0; i < columnCount; i++) {
            if(relatedColumn.equalsIgnoreCase(metaData.getColumnName( i + 1))){
                relatedColumnType = metaData.getColumnType(i + 1);
            }
            columns.append("`")
                .append(metaData.getColumnName( i + 1))
                .append("`, ");
        }
        //now we're going to build the values for data insertion
        parentResultSet.beforeFirst();
        while(parentResultSet.next()) {
            sql.append("INSERT INTO `").append(parentTable).append("`(");
            sql.append(columns.toString());
            //remove the last whitespace and comma
            sql.deleteCharAt(sql.length() - 1).deleteCharAt(sql.length() - 1).append(") VALUES \n");
            sql.append("(");
            String relatedColumnValue = null;
            for(int i = 0; i < columnCount; i++) {

                int columnType = metaData.getColumnType(i + 1);
                int columnIndex = i + 1;


                //this is the part where the values are processed based on their type
                if(Objects.isNull(parentResultSet.getObject(columnIndex))) {
                    sql.append("").append(parentResultSet.getObject(columnIndex)).append(", ");
                }
                else if( columnType == Types.INTEGER || columnType == Types.TINYINT || columnType == Types.BIT) {
                    sql.append(parentResultSet.getInt(columnIndex)).append(", ");
                }
                else {

                    String val = parentResultSet.getString(columnIndex);
                    //escape the single quotes that might be in the value
                    val = val.replace("'", "\\'");

                    sql.append("'").append(val).append("', ");
                }

                if((relatedColumnType == Types.INTEGER || relatedColumnType == Types.TINYINT || relatedColumnType == Types.BIT) && relatedColumn.equalsIgnoreCase(metaData.getColumnName(i + 1))){
                    relatedColumnValue = String.valueOf(parentResultSet.getInt(columnIndex));
                }else if(relatedColumnType == Types.VARCHAR && relatedColumn.equalsIgnoreCase(metaData.getColumnName(i + 1)) ){
                    relatedColumnValue = parentResultSet.getString(columnIndex);
                }
            }

            //now that we're done with a row
            //let's remove the last whitespace and comma
            sql.deleteCharAt(sql.length() - 1).deleteCharAt(sql.length() - 1);

            //if this is the last row, just append a closing
            //parenthesis otherwise append a closing parenthesis and a comma
            //for the next set of values
            sql.append(");");
            sql.append("\n--\n");
            //End of processing a Single Parent row


            ResultSet childTableResultSet = getResultSetFromChildTable(childTable,relatedColumn,relatedColumnValue);

            ResultSetMetaData childTableMetaData = childTableResultSet.getMetaData();
            int childTableColumnCount = childTableMetaData.getColumnCount();
                StringBuilder childTableColumns = new StringBuilder();
            //generate the column names that are present
            //in the returned result set
            //at this point the insert is INSERT INTO (`col1`, `col2`, ...)
            for(int i = 0; i < childTableColumnCount; i++) {
                childTableColumns.append("`")
                    .append(childTableMetaData.getColumnName( i + 1))
                    .append("`, ");
            }

            //now we're going to build the values for data insertion
            childTableResultSet.beforeFirst();

            while(childTableResultSet.next()) {
                sql.append("INSERT INTO `").append(childTable).append("`(");

                sql.append(childTableColumns.toString());
                //remove the last whitespace and comma
                sql.deleteCharAt(sql.length() - 1).deleteCharAt(sql.length() - 1).append(") VALUES \n");

                sql.append("(");
                for (int i = 0; i < columnCount; i++) {

                    int columnType = childTableMetaData.getColumnType(i + 1);
                    int columnIndex = i + 1;

                    //this is the part where the values are processed based on their type
                    if (Objects.isNull(childTableResultSet.getObject(columnIndex))) {
                        sql.append("").append(childTableResultSet.getObject(columnIndex)).append(", ");
                    } else if (columnType == Types.INTEGER || columnType == Types.TINYINT
                        || columnType == Types.BIT) {
                        sql.append(childTableResultSet.getInt(columnIndex)).append(", ");
                    } else {

                        String val = childTableResultSet.getString(columnIndex);
                        //escape the single quotes that might be in the value
                        val = val.replace("'", "\\'");

                        sql.append("'").append(val).append("', ");
                    }
                }

                //now that we're done with a row
                //let's remove the last whitespace and comma
                sql.deleteCharAt(sql.length() - 1).deleteCharAt(sql.length() - 1);

                //if this is the last row, just append a closing
                //parenthesis otherwise append a closing parenthesis and a comma
                //for the next set of values
                sql.append(");");
                sql.append("\n--\n");
            }
            childTableResultSet.close();
        }

        sql.append("\n--\n")
            .append(Utility.SQL_END_PATTERN).append(" table insert : ").append(parentTable)
            .append("\n--\n");
        sql.append("\n--\n")
            .append(Utility.SQL_END_PATTERN).append(" table insert : ").append(childTable)
            .append("\n--\n");

        //enable FK constraint
        sql.append("\n/*!40000 ALTER TABLE `").append(parentTable).append("` ENABLE KEYS */;\n");
        sql.append("\n/*!40000 ALTER TABLE `").append(childTable).append("` ENABLE KEYS */;\n");

        return sql.toString();
    }

    private ResultSet getResultSetFromChildTable(String childTable, String relatedColumn,String relatedValue)
        throws SQLException {
        String query = null;
        ResultSet rs = null;
        stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        //Build query to get the result set from the parent table
        if(StringUtils.hasLength(relatedColumn) && StringUtils.hasLength(relatedValue)){
            query = "SELECT * FROM " + "`" + childTable + "`" + " WHERE "+"`" + relatedColumn + "`"+ "="  + "'" + relatedValue + "'" + ";";
        }else{
            query = "SELECT * FROM " + "`" + childTable + "`;";

        }
        return  stmt.executeQuery(query);
    }

    private ResultSet getResultSetFromParentTable(String parentTable, String fromCreatedDateTime, String toCreatedDateTime)
        throws SQLException {
        stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        String query = null;
        ResultSet rs = null;
        //Build query to get the result set from the parent table
        if(StringUtils.hasLength(fromCreatedDateTime) && StringUtils.hasLength(toCreatedDateTime)){
            query = "SELECT * FROM " + "`" + parentTable + "`" + " WHERE created_ts >="  + "'" + fromCreatedDateTime + "'" +" AND created_ts <= "+"'" + toCreatedDateTime + "'"+";";
        }else{
            query = "SELECT * FROM " + "`" + parentTable + "`;";

        }
        return  stmt.executeQuery(query);
    }

    /**
     * This function will generate the insert statements needed
     * to recreate the table under processing.
     * @param table the table to get inserts statement for
     * @return String generated SQL insert
     * @throws SQLException exception
     */
    private String getDataInsertStatement(String table) throws SQLException {

        StringBuilder sql = new StringBuilder();

        ResultSet rs = stmt.executeQuery("SELECT * FROM " + "`" + table + "`;");

        //move to the last row to get max rows returned
        rs.last();
        int rowCount = rs.getRow();

        //there are no records just return empty string
        if(rowCount <= 0) {
            return sql.toString();
        }

        sql.append("\n--").append("\n-- Inserts of ").append(table).append("\n--\n\n");

        //temporarily disable foreign key constraint
        sql.append("\n/*!40000 ALTER TABLE `").append(table).append("` DISABLE KEYS */;\n");

        sql.append("\n--\n")
            .append(Utility.SQL_START_PATTERN).append(" table insert : ").append(table)
            .append("\n--\n");

        sql.append("INSERT INTO `").append(table).append("`(");

        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        //generate the column names that are present
        //in the returned result set
        //at this point the insert is INSERT INTO (`col1`, `col2`, ...)
        for(int i = 0; i < columnCount; i++) {
            sql.append("`")
                .append(metaData.getColumnName( i + 1))
                .append("`, ");
        }

        //remove the last whitespace and comma
        sql.deleteCharAt(sql.length() - 1).deleteCharAt(sql.length() - 1).append(") VALUES \n");

        //now we're going to build the values for data insertion
        rs.beforeFirst();
        while(rs.next()) {
            sql.append("(");
            for(int i = 0; i < columnCount; i++) {

                int columnType = metaData.getColumnType(i + 1);
                int columnIndex = i + 1;

                //this is the part where the values are processed based on their type
                if(Objects.isNull(rs.getObject(columnIndex))) {
                    sql.append("").append(rs.getObject(columnIndex)).append(", ");
                }
                else if( columnType == Types.INTEGER || columnType == Types.TINYINT || columnType == Types.BIT) {
                    sql.append(rs.getInt(columnIndex)).append(", ");
                }
                else {

                    String val = rs.getString(columnIndex);
                    //escape the single quotes that might be in the value
                    val = val.replace("'", "\\'");

                    sql.append("'").append(val).append("', ");
                }
            }

            //now that we're done with a row
            //let's remove the last whitespace and comma
            sql.deleteCharAt(sql.length() - 1).deleteCharAt(sql.length() - 1);

            //if this is the last row, just append a closing
            //parenthesis otherwise append a closing parenthesis and a comma
            //for the next set of values
            if(rs.isLast()) {
                sql.append(")");
            } else {
                sql.append("),\n");
            }
        }

        //now that we are done processing the entire row
        //let's add the terminator
        sql.append(";");

        sql.append("\n--\n")
            .append(Utility.SQL_END_PATTERN).append(" table insert : ").append(table)
            .append("\n--\n");

        //enable FK constraint
        sql.append("\n/*!40000 ALTER TABLE `").append(table).append("` ENABLE KEYS */;\n");

        return sql.toString();
    }


    /**
     * This is the entry function that'll
     * coordinate getTableInsertStatement() and getDataInsertStatement()
     * for every table in the database to generate a whole
     * script of SQL
     * @return String
     * @throws SQLException exception
     */
    private String exportToSql(String parentTable,String childTable) throws SQLException {

        StringBuilder sql = new StringBuilder();
        sql.append("--");
        sql.append("\n-- Date: ").append(new SimpleDateFormat("d-M-Y H:m:s").format(new java.util.Date()));
        sql.append("\n--");

        //these declarations are extracted from HeidiSQL
        sql.append("\n\n/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;")
            .append("\n/*!40101 SET NAMES utf8 */;")
            .append("\n/*!50503 SET NAMES utf8mb4 */;")
            .append("\n/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;")
            .append("\n/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;");

        TablesResponse tablesResponse = Utility.getTables(database, stmt,parentTable,childTable);

        List<String> tables = tablesResponse.getTables();
        try {
            if(dbProperties.getCreateTableIfNotExisits()){
                sql.append(getTableInsertStatement(parentTable));
                sql.append(getTableInsertStatement(childTable));

            }
            sql.append(getDataInsertStatement(parentTable,childTable,"customer_transaction_ref","2021-01-18 21:21:51","2021-01-18 22:21:55"));
        }catch (SQLException e){
            logger.error("Exception occurred while processing export for tables {} {} with error {} : ",parentTable,childTable, e);
        }

        sql.append("\n/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;")
            .append("\n/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;")
            .append("\n/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;");

        this.generatedSql = sql.toString();
        return sql.toString();
    }




    /**
     * This is the entry point for exporting
     * the database. It performs validation and
     * the initial object initializations,
     * database connection and setup
     * before ca
     * @throws IOException exception
     * @throws SQLException exception
     * @throws ClassNotFoundException exception
     */
    public void export() throws IOException, SQLException, ClassNotFoundException {
        logger.debug("Initiating the export with the following Database properties {} \n",dbProperties.toString());
        //check if properties is set or not
        if(!isValidateProperties()) {
            logger.error("Invalid config properties: The config properties is missing important parameters: DB_NAME, DB_USERNAME and DB_PASSWORD");
            return;
        }

        //connect to the database
        database = dbProperties.getName();
        String jdbcURL = dbProperties.getJdbcUrl();
        String driverName = dbProperties.getJdbcDriver();

        //Connection connection;

        if(jdbcURL.isEmpty()) {
            connection = Utility.connect(dbProperties.getUsername(), dbProperties.getPassword(),
                database, driverName);
        }
        else {
            if (jdbcURL.contains("?")) {
                database = jdbcURL.substring(jdbcURL.lastIndexOf("/") + 1, jdbcURL.indexOf("?"));
            } else {
                database = jdbcURL.substring(jdbcURL.lastIndexOf("/") + 1);
            }
            logger.debug("database name extracted from connection string: " + database);
            connection = Utility.connectWithURL(dbProperties.getUsername(), dbProperties.getPassword(),
                jdbcURL, driverName);
        }

        stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        String sql = exportToSql(dbProperties.getParentTable(),dbProperties.getChildTable());

        //close the statement
        stmt.close();

        //close the connection
        connection.close();

        logger.debug("Closing the connection ");
        //create a temp dir to store the exported file for processing
        dirName = StringUtils.hasLength(dbProperties.getExportDir()) ? dbProperties.getExportDir() : dirName;
        File file = new File(dirName);
        if(!file.exists()) {
            boolean res = file.mkdir();
            if(!res) {
                throw new IOException(LOG_PREFIX + ": Unable to create temp dir: " + file.getAbsolutePath());
            }
        }

        //write the sql file out
        File sqlFolder = new File(dirName + "/sql");
        if(!sqlFolder.exists()) {
            boolean res = sqlFolder.mkdir();
            if(!res) {
                throw new IOException(LOG_PREFIX + ": Unable to create temp dir: " + file.getAbsolutePath());
            }
        }

        sqlFileName = getSqlFilename();
        logger.debug("Writing the dump to a file at {} ",sqlFolder + "/" + sqlFileName);
        FileOutputStream outputStream = new FileOutputStream( sqlFolder + "/" + sqlFileName);
        outputStream.write(sql.getBytes());
        outputStream.close();
        logger.debug("Dump has been successfully created at {} ",sqlFolder + "/" + sqlFileName);

        //zip the file
        zipFileName = dirName + "/" + sqlFileName.replace(".sql", ".zip");
        generatedZipFile = new File(zipFileName);
        logger.debug("Creating a zip file at {} ",zipFileName);
        ZipUtil.pack(sqlFolder, generatedZipFile);
        logger.debug("Zip file has been successfully created at {} ",zipFileName);


    }



    /**
     * This will get the final output
     * sql file name.
     * @return String
     */
    public String getSqlFilename(){
        return isSqlFileNamePropertySet() ? dbProperties.getSqlFileName() + ".sql" :
            new SimpleDateFormat("d_M_Y_H_mm_ss").format(new Date()) + "_" + database + "_database_dump.sql";
    }

    public String getSqlFileName() {
        return sqlFileName;
    }

    /**
     * this is a getter for the raw sql generated in the backup process
     * @return generatedSql
     */
    public String getGeneratedSql() {
        return generatedSql;
    }

    /**
     * this is a getter for the generatedZipFile generatedZipFile File object
     * The reference can be used for further processing in
     * external systems
     * @return generatedZipFile or null
     */
    public File getGeneratedZipFile() {
        if(generatedZipFile != null && generatedZipFile.exists()) {
            return generatedZipFile;
        }
        return null;
    }

}
