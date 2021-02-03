package com.utility.dbdumprestore;


import com.utility.dbdumprestore.model.DbExportProperties;
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
import java.util.stream.Collectors;

@Component
public class DbExport {

    private final DbExportProperties dbProperties;

    private final Utility utility;



    public DbExport(DbExportProperties dbProperties, Utility utility){
        this.dbProperties = dbProperties;
        this.utility = utility;
    }
    private Statement stmt;

    private Connection connection;
    private String generatedSql = "";
    private Logger logger = LoggerFactory.getLogger(getClass());
    private final String LOG_PREFIX = "DB-Export: ";
    private String dirName = "DUMP";
    private String sqlFileName = "";
    private String zipFileName = "";
    private File generatedZipFile;
    private File sqlFolder;

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
                if(addIfNotExists) {
                    query = query.trim().replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
                }

                sql.append(query).append(";\n\n");
            }
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
    private void generateInsertStatements(String parentTable,List<String> childTables,String relatedColumn,String fromCreatedDateTime,String toCreatedDateTime)
        throws SQLException, IOException {

        StringBuilder sql = new StringBuilder();
        ResultSet parentResultSet = null;
        String query = null;

        parentResultSet = getResultSetFromParentTable(parentTable,fromCreatedDateTime,toCreatedDateTime);

        //move to the last row to get max rows returned
        parentResultSet.last();
        int rowCount = parentResultSet.getRow();

        //there are no records in the parent table just return empty string
        if(rowCount <= 0) {
            logger.debug("No records found for the table {} ",parentTable);
        }

        createExportDirIfNotExists();

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
        int recordCount = 0;
        int initialRecordCount = 1;
        int transactionCount = recordCount;
        int maxTransactionsPerFile = dbProperties.getTransactionsPerFile() == 0 ? rowCount : dbProperties.getTransactionsPerFile();
        while(parentResultSet.next()) {
            recordCount++;
            transactionCount++;
            sql.append("-- Record ").append(recordCount);
            sql.append("\n--\n");
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
            sql.append("\n");
            //End of processing a Single Parent row

            for (String childTable:childTables) {

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
                    for (int i = 0; i < childTableColumnCount; i++) {

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
                    sql.append("\n");
                }
                childTableResultSet.close();
            }
            sql.append("-- Record ").append(recordCount).append(" Ends --");
            writeToSqlFile(sql.toString(), initialRecordCount +"-"+maxTransactionsPerFile);
            sql = new StringBuilder();
            if(transactionCount == maxTransactionsPerFile){
                logger.debug("Dump file has been successfully created at {} ",sqlFolder + "/" + sqlFileName);
                compressSqlFile();
                //writeToSqlFile(sql.toString(), initialRecordCount +"-"+recordCount);
                initialRecordCount = recordCount;
                transactionCount = 0;
                sql = new StringBuilder();
            }
        }
        /*if(StringUtils.hasLength(sql.toString())){
            writeToSqlFile(sql.toString(), initialRecordCount +"-"+maxTransactionsPerFile);
        }*/
    }

    private void createExportDirIfNotExists() throws IOException {
        dirName = StringUtils.hasLength(dbProperties.getExportDir()) ? dbProperties.getExportDir() : dirName;
        File file = new File(dirName);
        if(!file.exists()) {
            boolean res = file.mkdir();
            if(!res) {
                throw new IOException(LOG_PREFIX + ": Unable to create temp dir: " + file.getAbsolutePath());
            }
        }

        //write the sql file out
        sqlFolder = new File(dirName + "/sql");
        if(!sqlFolder.exists()) {
            boolean res = sqlFolder.mkdir();
            if(!res) {
                throw new IOException(LOG_PREFIX + ": Unable to create temp dir: " + file.getAbsolutePath());
            }
        }
    }

    private void writeToSqlFile(String sql,String fileNameSuffix) throws IOException {
        sqlFileName = getSqlFilename();
        sqlFileName = StringUtils.hasLength(fileNameSuffix) ?sqlFileName+"_"+fileNameSuffix+".sql": sqlFileName+".sql";
        //logger.debug("Writing the sql to  {} ",sqlFolder + "/" + sqlFileName);
        try (FileOutputStream outputStream = new FileOutputStream(sqlFolder + "/" + sqlFileName,true)) {
            outputStream.write(sql.getBytes());
            outputStream.close();
        }
    }

    private void compressSqlFile() throws IOException {
        zipFileName = dirName + "/" + sqlFileName.replace(".sql", ".zip");
        File sqlFolder = new File(dirName + "/sql");
        generatedZipFile = new File(zipFileName);
        logger.debug("Creating a zip file at {} ",zipFileName);
        ZipUtil.pack(sqlFolder, generatedZipFile);
        logger.debug("Zip file has been successfully created at {} ",zipFileName);

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
        if(!utility.isValidateProperties()) {
            logger.error("Invalid config properties: The config properties is missing important parameters: DB_NAME, DB_USERNAME and DB_PASSWORD");
            return;
        }

        //connect to the database
        connection = utility.getConnection();
        stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        generateInsertStatements(dbProperties.getParentTable(),dbProperties.getChildTables(),dbProperties.getRelatedColumn(),dbProperties.getFromCreatedDateTime(),dbProperties.getToCreatedDateTime());

        //close the statement
        stmt.close();

        //close the connection
        connection.close();

        logger.debug("Closing the connection ");
        logger.debug("Export has been completed successfully");

    }


    /**
     * This will get the final output
     * sql file name.
     * @return String
     */
    public String getSqlFilename(){
        return utility.isSqlFileNamePropertySet() ? dbProperties.getSqlFileName():
            new SimpleDateFormat("d_M_Y_H_mm_ss").format(new Date())  + "_database_dump";
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
