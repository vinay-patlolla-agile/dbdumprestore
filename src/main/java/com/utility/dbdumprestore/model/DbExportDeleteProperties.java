package com.utility.dbdumprestore.model;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * @author vinay
 */
@Configuration
@ConfigurationProperties(prefix = "db.export.delete")
public class DbExportDeleteProperties
{
    private String dbName;
    private String username;
    private String password;
    private String jdbcUrl;
    private String jdbcDriver;

    private String parentTable;
    private List<String> childTables;
    private String relatedColumn;
    private String exportDir;
    private String sqlFileName;
    private int transactionsPerFile;
    private Boolean createTableIfNotExisits;
    private String fromCreatedDateTime;
    private String toCreatedDateTime;

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public String getParentTable() {
        return parentTable;
    }

    public void setParentTable(String parentTable) {
        this.parentTable = parentTable;
    }

    public List<String> getChildTables() {
        return childTables;
    }

    public void setChildTables(List<String> childTables) {
        this.childTables = childTables;
    }

    public String getRelatedColumn() {
        return relatedColumn;
    }

    public void setRelatedColumn(String relatedColumn) {
        this.relatedColumn = relatedColumn;
    }

    public String getExportDir() {
        return exportDir;
    }

    public void setExportDir(String exportDir) {
        this.exportDir = exportDir;
    }

    public String getSqlFileName() {
        return sqlFileName;
    }

    public void setSqlFileName(String sqlFileName) {
        this.sqlFileName = sqlFileName;
    }

    public int getTransactionsPerFile() {
        return transactionsPerFile;
    }

    public void setTransactionsPerFile(int transactionsPerFile) {
        this.transactionsPerFile = transactionsPerFile;
    }

    public Boolean getCreateTableIfNotExisits() {
        return createTableIfNotExisits;
    }

    public void setCreateTableIfNotExisits(Boolean createTableIfNotExisits) {
        this.createTableIfNotExisits = createTableIfNotExisits;
    }

    public String getFromCreatedDateTime() {
        return fromCreatedDateTime;
    }

    public void setFromCreatedDateTime(String fromCreatedDateTime) {
        this.fromCreatedDateTime = fromCreatedDateTime;
    }

    public String getToCreatedDateTime() {
        return toCreatedDateTime;
    }

    public void setToCreatedDateTime(String toCreatedDateTime) {
        this.toCreatedDateTime = toCreatedDateTime;
    }
}
