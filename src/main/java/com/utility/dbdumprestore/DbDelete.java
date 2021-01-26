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

@Component
public class DbDelete {

    private DbExportProperties dbProperties;

    private Utility utility;

    private Connection connection;

    private Statement stmt;
    private String dirName = "DELETE_DUMP";

    private Logger logger = LoggerFactory.getLogger(getClass());

    public DbDelete(DbExportProperties dbProperties, Utility utility){
        this.dbProperties = dbProperties;
        this.utility = utility;
    }

    public boolean exportDeleteStatements() throws SQLException, ClassNotFoundException, IOException {
        //check if properties is set or not
        if(!utility.isValidateProperties()) {
            logger.error("Invalid config properties: The config properties is missing important parameters: DB_NAME, DB_USERNAME and DB_PASSWORD");
            return false;
        }

        connection = utility.getConnection();
        stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);


        String sql = getDataDeleteStatement(dbProperties.getParentTable(),dbProperties.getChildTables(),dbProperties.getRelatedColumn(),dbProperties.getFromCreatedDateTime(),dbProperties.getToCreatedDateTime());

        //close the statement
        stmt.close();

        //close the connection
        connection.close();

        //create a temp dir to store the exported file for processing
        dirName = StringUtils.hasLength(dbProperties.getExportDir()) ? dbProperties.getExportDir() : dirName;
        File file = new File(dirName);
        String LOG_PREFIX = "DB-DELETE-EXPORT: ";
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

        String sqlFileName = getSqlFilename();
        logger.debug("Writing the delete statements dump to a file at {} ",sqlFolder + "/" + sqlFileName);
        FileOutputStream outputStream = new FileOutputStream( sqlFolder + "/" + sqlFileName);
        outputStream.write(sql.getBytes());
        outputStream.close();
        logger.debug("Delete statements Dump has been successfully created at {} ",sqlFolder + "/" + sqlFileName);

        //zip the file
        String zipFileName = dirName + "/" + sqlFileName.replace(".sql", ".zip");
        File generatedZipFile = new File(zipFileName);
        logger.debug("Creating a zip file at {} ", zipFileName);
        ZipUtil.pack(sqlFolder, generatedZipFile);
        logger.debug("Zip file has been successfully created at {} ", zipFileName);

        return true;

    }

    /**
     * This will get the final output
     * sql file name.
     * @return String
     */
    public String getSqlFilename(){
        return utility.isSqlFileNamePropertySet() ? dbProperties.getSqlFileName() + ".sql" :
            new SimpleDateFormat("d_M_Y_H_mm_ss").format(new Date())  + "_delete_database_dump.sql";
    }

    private boolean deleteFrom(String fromCreatedDateTime, String toCreatedDateTime) throws SQLException {

        stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        String query = null;
        ResultSet rs = null;
        //Build query to get the result set from the parent table
        if(StringUtils.hasLength(fromCreatedDateTime) && StringUtils.hasLength(toCreatedDateTime)){
            query = "SELECT * FROM " + "`" + dbProperties.getParentTable() + "`" + " WHERE created_ts >="  + "'" + fromCreatedDateTime + "'" +" AND created_ts <= "+"'" + toCreatedDateTime + "'"+";";
        }else{
            query = "SELECT * FROM " + "`" + dbProperties.getParentTable() + "`;";
        }
        ResultSet resultSet = stmt.executeQuery(query);

        //move to the last row to get max rows returned
        resultSet.last();
        int rowCount = resultSet.getRow();

        //there are no records in the parent table just return empty string
        if(rowCount <= 0) {
            logger.debug("No records found for the table to delete",resultSet);
            return false;
        }

        resultSet.beforeFirst();
        int recordCount = 0;
        while(resultSet.next()) {
            recordCount++;

        }

        return true;


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
     * This function will generate the delete statements
     * @param parentTable the table to get delete statement for
     * @return String generated SQL insert
     * @throws SQLException exception
     */
    private String getDataDeleteStatement(String parentTable, List<String> childTables,String relatedColumn,String fromCreatedDateTime,String toCreatedDateTime) throws SQLException {

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

        //temporarily disable foreign key constraint
        sql.append("\n/*!40000 ALTER TABLE `").append(parentTable).append("` DISABLE KEYS */;\n");
        childTables.stream().forEach(table->{
            sql.append("\n/*!40000 ALTER TABLE `").append(table).append("` DISABLE KEYS */;\n");
        });
        //sql.append("\n/*!40000 ALTER TABLE `").append(childTable).append("` DISABLE KEYS */;\n");

        ResultSetMetaData metaData = parentResultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        int relatedColumnType = Types.VARCHAR;
        StringBuilder parentTableSql = new StringBuilder();

        //generate the column names that are present
        //in the returned result set
        //at this point the insert is INSERT INTO (`col1`, `col2`, ...)
        for(int i = 0; i < columnCount; i++) {
            if(relatedColumn.equalsIgnoreCase(metaData.getColumnName( i + 1))){
                relatedColumnType = metaData.getColumnType(i + 1);
            }
        }
        //now we're going to build the values for data insertion
        parentResultSet.beforeFirst();
        int recordCount = 0;
        while(parentResultSet.next()) {
            recordCount++;
            sql.append("-- Record ").append(recordCount);
            sql.append("\n--\n");
            parentTableSql.append("DELETE FROM `").append(parentTable).append("` WHERE `").append(relatedColumn).append("`= ");

            String relatedColumnValue = null;
            for(int i = 0; i < columnCount; i++) {

                int columnType = metaData.getColumnType(i + 1);
                int columnIndex = i + 1;

                if((relatedColumnType == Types.INTEGER || relatedColumnType == Types.TINYINT || relatedColumnType == Types.BIT) && relatedColumn.equalsIgnoreCase(metaData.getColumnName(i + 1))){
                    relatedColumnValue = String.valueOf(parentResultSet.getInt(columnIndex));
                    parentTableSql.append("" + relatedColumnValue + "");
                    break;
                }else if(relatedColumnType == Types.VARCHAR && relatedColumn.equalsIgnoreCase(metaData.getColumnName(i + 1)) ){
                    relatedColumnValue = parentResultSet.getString(columnIndex);
                    parentTableSql.append("'" + relatedColumnValue + "'");
                    break;
                }

            }
            parentTableSql.append(";");
            parentTableSql.append("\n");
            //End of processing a Single Parent row

            for (String childTable:childTables) {

                ResultSet childTableResultSet = getResultSetFromChildTable(childTable,relatedColumn,relatedColumnValue);

                ResultSetMetaData childTableMetaData = childTableResultSet.getMetaData();
                int childTableColumnCount = childTableMetaData.getColumnCount();
                StringBuilder childTableColumns = new StringBuilder();
                //now we're going to build the values for data insertion
                childTableResultSet.beforeFirst();

                while(childTableResultSet.next()) {
                    sql.append("DELETE FROM `").append(childTable).append("` WHERE `").append(relatedColumn).append("`= ");
                    //sql.append("INSERT INTO `").append(childTable).append("`(");
                    for (int i = 0; i < childTableColumnCount; i++) {

                        int columnType = childTableMetaData.getColumnType(i + 1);
                        int columnIndex = i + 1;

                        if((relatedColumnType == Types.INTEGER || relatedColumnType == Types.TINYINT || relatedColumnType == Types.BIT) && relatedColumn.equalsIgnoreCase(childTableMetaData.getColumnName(i + 1))){
                            relatedColumnValue = String.valueOf(childTableResultSet.getInt(columnIndex));
                            sql.append("" + relatedColumnValue + "");
                            break;
                        }else if(relatedColumnType == Types.VARCHAR && relatedColumn.equalsIgnoreCase(childTableMetaData.getColumnName(i + 1)) ){
                            relatedColumnValue = childTableResultSet.getString(columnIndex);
                            sql.append("'" + relatedColumnValue + "'");
                            break;
                        }

                    }
                    sql.append(";");
                    sql.append("\n");
                }
                childTableResultSet.close();
            }
            sql.append(parentTableSql.toString());
            sql.append("-- Record ").append(recordCount).append(" Ends --");
        }
        //enable FK constraint
        sql.append("\n/*!40000 ALTER TABLE `").append(parentTable).append("` ENABLE KEYS */;\n");
        childTables.stream().forEach(table->{
            sql.append("\n/*!40000 ALTER TABLE `").append(table).append("` ENABLE KEYS */;\n");
        });

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

}
