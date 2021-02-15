package com.utility.dbdumprestore;


import com.utility.dbdumprestore.model.DbExportDeleteProperties;
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

@Component
public class DbDelete {

    private final DbExportDeleteProperties dbProperties;

    private final Utility utility;

    private Connection connection;

    private String sqlFileName;

    public DbDelete(DbExportDeleteProperties dbProperties, Utility utility){
        this.dbProperties = dbProperties;
        this.utility = utility;
    }

    private String generatedSql = "";
    private Logger logger = LoggerFactory.getLogger(getClass());
    private final String LOG_PREFIX = "DB-Delete-Export: ";
    private String dirName = "DUMP";
    private String zipFileName = "";
    private File generatedZipFile;
    private File sqlFolder;
    private static final String dateToday = LocalDateTime.now().toString();


    /**
     * This function will generate the delete statements needed
     * to recreate the table under processing.
     * @param parentTable the table to get inserts statement for
     * @return String generated SQL insert
     * @throws SQLException exception
     */
    private void generateDeleteStatements(String parentTable,List<String> childTables,String relatedColumn,String fromCreatedDateTime,String toCreatedDateTime)
        throws SQLException, IOException, ClassNotFoundException {

        StringBuilder sql = new StringBuilder();
        ResultSet parentResultSet = null;
        ResultSet childTableResultSet = null;
        Statement stmt = null;
        String queryParentTable =getQuery(parentTable,fromCreatedDateTime,toCreatedDateTime);
        try {
            connection = utility.getExportDeleteConnection();
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

            for(int i = 0; i < columnCount; i++) {
                if(relatedColumn.equalsIgnoreCase(metaData.getColumnName( i + 1))){
                    relatedColumnType = metaData.getColumnType(i + 1);
                    break;
                }
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
                recordBuilder.append("-- Record ");
                sql.append("DELETE FROM `").append(parentTable).append("` WHERE `").append(relatedColumn).append("`= ");

                String relatedColumnValue = null;
                for(int i = 0; i < columnCount; i++) {

                    int columnType = metaData.getColumnType(i + 1);
                    int columnIndex = i + 1;

                    if((relatedColumnType == Types.INTEGER || relatedColumnType == Types.TINYINT || relatedColumnType == Types.BIT) && relatedColumn.equalsIgnoreCase(metaData.getColumnName(i + 1))){
                        relatedColumnValue = String.valueOf(parentResultSet.getInt(columnIndex));
                        sql.append("" + relatedColumnValue + "");

                    }else if(relatedColumnType == Types.VARCHAR && relatedColumn.equalsIgnoreCase(metaData.getColumnName(i + 1)) ){
                        relatedColumnValue = parentResultSet.getString(columnIndex);
                        sql.append("'" + relatedColumnValue + "'");

                    }

                    if("customer_transaction_ref".equalsIgnoreCase(metaData.getColumnName(columnIndex))){
                        recordBuilder.append(parentResultSet.getString(columnIndex));
                    }else if("tracking_number".equalsIgnoreCase(metaData.getColumnName(columnIndex))){
                        if(StringUtils.hasLength(parentResultSet.getString(columnIndex)) && ! recordBuilder.toString().contains("-tno"+parentResultSet.getString(columnIndex))){
                            recordBuilder.append("-").append("tno").append(parentResultSet.getString(columnIndex));
                        }
                    }

                }
                sql.append(";");
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

                    //now we're going to build the values for data insertion
                    childTableResultSet.beforeFirst();

                    while(childTableResultSet.next()) {
                        numberOfRecordsExported++;
                        sql.append("DELETE FROM `").append(childTable).append("` WHERE `").append(relatedColumn).append("`= ");
                        for (int i = 0; i < childTableColumnCount; i++) {

                            int columnType = childTableMetaData.getColumnType(i + 1);
                            int columnIndex = i + 1;

                            if((relatedColumnType == Types.INTEGER || relatedColumnType == Types.TINYINT || relatedColumnType == Types.BIT) && relatedColumn.equalsIgnoreCase(childTableMetaData.getColumnName(i + 1))){
                                relatedColumnValue = String.valueOf(childTableResultSet.getInt(columnIndex));
                                sql.append("" + relatedColumnValue + "");
                            }else if(relatedColumnType == Types.VARCHAR && relatedColumn.equalsIgnoreCase(childTableMetaData.getColumnName(i + 1)) ){
                                relatedColumnValue = childTableResultSet.getString(columnIndex);
                                sql.append("'" + relatedColumnValue + "'");
                            }

                            if("tracking_number".equalsIgnoreCase(childTableMetaData.getColumnName(columnIndex))){
                                if(StringUtils.hasLength(childTableResultSet.getString(columnIndex)) && ! recordBuilder.toString().contains("-tno"+childTableResultSet.getString(columnIndex))){
                                    recordBuilder.append("-").append("tno").append(childTableResultSet.getString(columnIndex));
                                }

                            }
                        }
                        sql.append(";");
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
            logger.info("Total number of delete statements processed were {} ",numberOfRecordsExported);
        }catch(Exception e){
            logger.error("Error export the delete statements {} ",e);
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
    public void exportDeleteStatements() throws Exception {
        StopWatch stopWatch=new StopWatch();
        logger.debug("Initiating the export with the following Database properties {} \n",dbProperties.toString());
        //check if properties is set or not
        if(!utility.isValidateDbExportDeleteProperties()) {
            logger.error("Invalid config properties: The config properties is missing important parameters: DB_NAME, DB_USERNAME and DB_PASSWORD");
            return;
        }
        stopWatch.start("total_execution_time");
        createExportDirIfNotExists();
        createSqlFileName();
        generateDeleteStatements(dbProperties.getParentTable(),dbProperties.getChildTables(),dbProperties.getRelatedColumn(),dbProperties.getFromCreatedDateTime(),dbProperties.getToCreatedDateTime());
        stopWatch.stop();
        logger.debug("Delete statements export has been completed successfully");
        logger.debug("Total execution time in minutes {} ",stopWatch.getTotalTimeSeconds() / 60);

    }

    private void createSqlFileName() {
        getSqlFilename();
    }


    /**
     * This will get the final output
     * sql file name.
     * @return String
     */
    public String getSqlFilename(){
        if(! StringUtils.hasLength(sqlFileName)){
            sqlFileName = dateToday  + "_delete_database_dump";
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
