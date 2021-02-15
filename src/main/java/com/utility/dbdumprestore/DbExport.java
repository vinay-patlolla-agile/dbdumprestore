package com.utility.dbdumprestore;


import com.utility.dbdumprestore.model.DbExportInsertProperties;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;
import org.springframework.util.StringUtils;
import org.zeroturnaround.zip.ZipUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

@Component
public class DbExport {

    private final DbExportInsertProperties dbProperties;

    private final Utility utility;

    private Connection connection;

    private String sqlFileName;



    public DbExport(DbExportInsertProperties dbProperties, Utility utility){
        this.dbProperties = dbProperties;
        this.utility = utility;
    }

    private String generatedSql = "";
    private Logger logger = LoggerFactory.getLogger(getClass());
    private final String LOG_PREFIX = "DB-Export: ";
    private String dirName = "DUMP";
    private String zipFileName = "";
    private File generatedZipFile;
    private File sqlFolder;
    private static final String dateToday = LocalDateTime.now().toString();


    /**
     * This function will generate the insert statements needed
     * to recreate the table under processing.
     * @param parentTable the table to get inserts statement for
     * @return String generated SQL insert
     * @throws SQLException exception
     */
    private void generateInsertStatements(String parentTable,List<String> childTables,String relatedColumn,String fromCreatedDateTime,String toCreatedDateTime)
        throws SQLException, IOException, ClassNotFoundException {

        StringBuilder sql = new StringBuilder();
        String[] insertColumnsStore=new String[childTables.size()];
        ResultSet parentResultSet = null;
        ResultSet childTableResultSet = null;
        Statement stmt = null;
        String queryParentTable =getQuery(parentTable,fromCreatedDateTime,toCreatedDateTime);
        try {
            stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            StopWatch parentQueryExecutionWatch = new StopWatch();
            parentQueryExecutionWatch.start();
            parentResultSet = stmt.executeQuery(queryParentTable);
            parentQueryExecutionWatch.stop();
            logger.info("Execution time for the query on parent table in seconds {} ",Precision.round(parentQueryExecutionWatch.getTotalTimeSeconds(),2));

            //move to the last row to get max rows returned
            parentResultSet.last();
            int rowCount = parentResultSet.getRow();

            //there are no records in the parent table just return empty string
            if(rowCount <= 0) {
                logger.debug("No records found for the table {} ",parentTable);
            }
            ResultSetMetaData metaData = parentResultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            int relatedColumnType = Types.VARCHAR;
            StringBuilder columns = new StringBuilder();

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
            int initialRecordCount = 0;
            int transactionCount = recordCount;
            int numberOfRecordsExported = 0;
            int maxTransactionsPerFile = dbProperties.getTransactionsPerFile() == 0 ? rowCount : dbProperties.getTransactionsPerFile();
            while(parentResultSet.next()) {
                numberOfRecordsExported++;
                recordCount++;
                transactionCount++;
                StringBuilder recordBuilder=new StringBuilder();
                //parentResultSet.getString()
                recordBuilder.append("-- Record ");
                sql.append("INSERT INTO `").append(parentTable).append("`(").append(columns.toString());
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

                    if("customer_transaction_ref".equalsIgnoreCase(metaData.getColumnName(columnIndex))){
                        recordBuilder.append(parentResultSet.getString(columnIndex));
                    }else if("tracking_number".equalsIgnoreCase(metaData.getColumnName(columnIndex))){
                        if(StringUtils.hasLength(parentResultSet.getString(columnIndex)) && ! recordBuilder.toString().contains("-tno"+parentResultSet.getString(columnIndex))){
                            recordBuilder.append("-").append("tno").append(parentResultSet.getString(columnIndex));
                        }
                    }
                    if((relatedColumnType == Types.INTEGER || relatedColumnType == Types.TINYINT || relatedColumnType == Types.BIT) && relatedColumn.equalsIgnoreCase(metaData.getColumnName(i + 1))){
                        relatedColumnValue = String.valueOf(parentResultSet.getInt(columnIndex));
                    }else if(relatedColumnType == Types.VARCHAR && relatedColumn.equalsIgnoreCase(metaData.getColumnName(i + 1)) ){
                        relatedColumnValue = parentResultSet.getString(columnIndex);
                    }
                }
                sql.deleteCharAt(sql.length() - 1).deleteCharAt(sql.length() - 1);
                sql.append(");");
                sql.append("\n");
                //End of processing a Single Parent row

                int childTableIndex = 0;
                StopWatch overrallChildTablesExecutionWatch = new StopWatch();
                overrallChildTablesExecutionWatch.start("Total Query time on child tables for key '"+relatedColumnValue+"'");
                for (String childTable:childTables) {
                    stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    String queryForChildTable = getQueryForChildTable(childTable,relatedColumn,relatedColumnValue);
                    StopWatch childTableQueryWatch = new StopWatch();
                    childTableQueryWatch.start("Query time on table '"+childTable+"' with key '"+relatedColumnValue+"'");

                    childTableResultSet = stmt.executeQuery(queryForChildTable);
                    childTableQueryWatch.stop();

                    logger.debug(childTableQueryWatch.getLastTaskName()+" is(in milliseconds ) :"+Precision.round(childTableQueryWatch.getTotalTimeMillis(),2));

                    ResultSetMetaData childTableMetaData = childTableResultSet.getMetaData();
                    int childTableColumnCount = childTableMetaData.getColumnCount();

                    if(! StringUtils.hasLength(insertColumnsStore[childTableIndex])){
                        StringBuilder childTableColumns = new StringBuilder();
                        for(int i = 0; i < childTableColumnCount; i++) {
                            childTableColumns.append("`")
                                .append(childTableMetaData.getColumnName( i + 1))
                                .append("`, ");
                        }
                        insertColumnsStore[childTableIndex] = childTableColumns.toString();
                    }

                    String columnsFromStore = insertColumnsStore[childTableIndex];
                    //now we're going to build the values for data insertion
                    childTableResultSet.beforeFirst();

                    while(childTableResultSet.next()) {
                        numberOfRecordsExported++;
                        sql.append("INSERT INTO `").append(childTable).append("`(").append(columnsFromStore.toString());
                        //remove the last whitespace and comma
                        sql.deleteCharAt(sql.length() - 1).deleteCharAt(sql.length() - 1).append(") VALUES \n").append("(");

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
                            if("tracking_number".equalsIgnoreCase(childTableMetaData.getColumnName(columnIndex))){
                                if(StringUtils.hasLength(childTableResultSet.getString(columnIndex)) && ! recordBuilder.toString().contains("-tno"+childTableResultSet.getString(columnIndex))){
                                    recordBuilder.append("-").append("tno").append(childTableResultSet.getString(columnIndex));
                                }

                            }
                        }
                        sql.deleteCharAt(sql.length() - 1).deleteCharAt(sql.length() - 1);
                        sql.append(");");
                        sql.append("\n");
                    }

                    childTableIndex++;
                }
                overrallChildTablesExecutionWatch.stop();
                logger.info(overrallChildTablesExecutionWatch.getLastTaskName()+" is(in milliseconds ) :"+Precision.round(overrallChildTablesExecutionWatch.getTotalTimeMillis(),2));
                recordBuilder.append("\n").append(sql);
                recordBuilder.append("-- Record ").append(" Ends\n");
                writeToSqlFile(recordBuilder.toString(), initialRecordCount == 0 ? 1 +"-"+(initialRecordCount+maxTransactionsPerFile) : (initialRecordCount + 1) +"-"+(initialRecordCount+maxTransactionsPerFile));
                sql = new StringBuilder();
                recordBuilder = new StringBuilder();
                if(transactionCount == maxTransactionsPerFile){
                    logger.debug("Dump file has been successfully created at {} ",sqlFolder + "/" + sqlFileName);
                    compressSqlFile();
                    initialRecordCount = recordCount;
                    transactionCount = 0;
                }
            }
            logger.info("Total number of records processed were {} ",numberOfRecordsExported);
        }catch(Exception e){
            logger.error("Error export the insert statments {} ",e);
            throw e;
        }finally{
            DbUtils.closeQuietly(parentResultSet);
            DbUtils.closeQuietly(childTableResultSet);
            DbUtils.closeQuietly(stmt);
            DbUtils.closeQuietly(connection);
        }


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

    private boolean deleteDirectoryFiles(File directoryToDelete){
        File[] allFiles = directoryToDelete.listFiles();
        if (allFiles != null) {
            for (File fileToDelete : allFiles) {
                deleteDirectoryFiles(fileToDelete);
            }
        }
        return directoryToDelete.delete();
    }

    private void writeToSqlFile(String sql,String fileNameSuffix) throws IOException {
        String filename = getSqlFilename();
        filename = StringUtils.hasLength(fileNameSuffix) ?sqlFileName+"_"+fileNameSuffix+".sql": sqlFileName+".sql";
        //logger.debug("Writing the sql to  {} ",sqlFolder + "/" + sqlFileName);
        try (FileOutputStream outputStream = new FileOutputStream(sqlFolder + "/" + filename,true)) {
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


    private String getQueryForChildTable(String childTable, String relatedColumn,String relatedValue)
        {
        String query = null;
        //Build query to get the result set from the parent table
        if(StringUtils.hasLength(relatedColumn) && StringUtils.hasLength(relatedValue)){
            query = "SELECT * FROM " + "`" + childTable + "`" + " WHERE "+"`" + relatedColumn + "`"+ "="  + "'" + relatedValue + "'" + ";";
        }else{
            query = "SELECT * FROM " + "`" + childTable + "`;";

        }
        return  query;
    }

    private String getQuery(String parentTable, String fromCreatedDateTime, String toCreatedDateTime){
        String query = null;
        ResultSet rs = null;
        //Build query to get the result set from the parent table
        if(StringUtils.hasLength(fromCreatedDateTime) && StringUtils.hasLength(toCreatedDateTime)){
            query = "SELECT * FROM " + "`" + parentTable + "`" + " WHERE created_ts >="  + "'" + fromCreatedDateTime + "'" +" AND created_ts <= "+"'" + toCreatedDateTime + "'"+";";
        }else{
            query = "SELECT * FROM " + "`" + parentTable + "`;";

        }
        return query;
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
    public void export() throws Exception {
        StopWatch stopWatch=new StopWatch();
        logger.debug("Initiating the export with the following Database properties {} \n",dbProperties.toString());
        //check if properties is set or not
        if(!utility.isValidateDbExportInsertProperties()) {
            logger.error("Invalid config properties: The config properties is missing important parameters: DB_NAME, DB_USERNAME and DB_PASSWORD");
            return;
        }
        stopWatch.start("total_execution_time");
        createExportDirIfNotExists();
        connection = utility.getExportInsertConnection();
        createSqlFileName();
        if(dbProperties.getCreateTableIfNotExisits()){
            logger.debug("Writing the create table statements");
            String createTableSql = getTableInsertStatement(dbProperties.getParentTable());
            writeToSqlFile(createTableSql,"create_tables");
            for(String childTable : dbProperties.getChildTables()){
                createTableSql = getTableInsertStatement(childTable);
                writeToSqlFile(createTableSql,"create_tables");
            }
            logger.debug("Completed writing the create table statements");
        }
        generateInsertStatements(dbProperties.getParentTable(),dbProperties.getChildTables(),dbProperties.getRelatedColumn(),dbProperties.getFromCreatedDateTime(),dbProperties.getToCreatedDateTime());
        stopWatch.stop();
        logger.debug("Export has been completed successfully");
        logger.debug("Total execution time in minutes {} ",stopWatch.getTotalTimeSeconds() / 60);

    }

    private void createSqlFileName() {
        getSqlFilename();
    }


    /**
     * This will generate the SQL statement
     * for creating the table supplied in the
     * method signature
     * @param table the table concerned
     * @return String
     * @throws SQLException exception
     */
    private String getTableInsertStatement(String table){

        StringBuilder createTableSql = new StringBuilder();
        ResultSet rs;
        Statement stmt = null;
        try {
            if(table != null && !table.isEmpty()){
                stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                rs = stmt.executeQuery("SHOW CREATE TABLE " + "`" + table + "`;");
                while ( rs.next() ) {
                    String qtbl = rs.getString(1);
                    String query = rs.getString(2);
                    createTableSql.append("\n--\n");
                    // sql.append("\n").append(MysqlBaseService.SQL_START_PATTERN).append("  table dump : ").append(qtbl);
                    query = query.trim().replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");

                    createTableSql.append(query).append(";\n");
                }
                //sql.append("\n").append(MysqlBaseService.SQL_END_PATTERN).append("  table dump : ").append(table);
                createTableSql.append("\n--\n");
            }
        }catch (SQLException  e) {
            logger.error("Error in executing the query {}",e);
        }finally {
            DbUtils.closeQuietly(stmt);
        }
        return createTableSql.toString();
    }

    /**
     * This will get the final output
     * sql file name.
     * @return String
     */
    public String getSqlFilename(){
        if(! StringUtils.hasLength(sqlFileName)){
            sqlFileName = dateToday  + "_database_dump";
        }
        return sqlFileName;
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
