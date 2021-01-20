package com.utility.dbdumprestore.model;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "db")
public class DbProperties {
    String name;
    String username;
    String password;
    String jdbcUrl;
    String jdbcDriver;
    String parentTable;
    String childTable;
    String exportDir;
    String sqlFileName;

    Boolean createTableIfNotExisits;

    String fromCreatedDateTime;
    String toCreatedDateTime;


    public String getToCreatedDateTime() {
        return toCreatedDateTime;
    }

    public void setToCreatedDateTime(String toCreatedDateTime) {
        this.toCreatedDateTime = toCreatedDateTime;
    }

    public String getFromCreatedDateTime() {
        return fromCreatedDateTime;
    }

    public void setFromCreatedDateTime(String fromCreatedDateTime) {
        this.fromCreatedDateTime = fromCreatedDateTime;
    }

    public Boolean getCreateTableIfNotExisits() {
        return createTableIfNotExisits;
    }

    public void setCreateTableIfNotExisits(Boolean createTableIfNotExisits) {
        this.createTableIfNotExisits = createTableIfNotExisits;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public String getChildTable() {
        return childTable;
    }

    public void setChildTable(String childTable) {
        this.childTable = childTable;
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

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DbProperties that = (DbProperties) o;

        if (name != null ? !name.equals(that.name) : that.name != null)
            return false;
        if (username != null ? !username.equals(that.username) : that.username != null)
            return false;
        if (password != null ? !password.equals(that.password) : that.password != null)
            return false;
        if (jdbcUrl != null ? !jdbcUrl.equals(that.jdbcUrl) : that.jdbcUrl != null)
            return false;
        if (jdbcDriver != null ? !jdbcDriver.equals(that.jdbcDriver) : that.jdbcDriver != null)
            return false;
        if (parentTable != null ? !parentTable.equals(that.parentTable) : that.parentTable != null)
            return false;
        if (childTable != null ? !childTable.equals(that.childTable) : that.childTable != null)
            return false;
        if (exportDir != null ? !exportDir.equals(that.exportDir) : that.exportDir != null)
            return false;
        if (sqlFileName != null ? !sqlFileName.equals(that.sqlFileName) : that.sqlFileName != null)
            return false;
        if (createTableIfNotExisits != null ?
            !createTableIfNotExisits.equals(that.createTableIfNotExisits) :
            that.createTableIfNotExisits != null)
            return false;
        if (fromCreatedDateTime != null ?
            !fromCreatedDateTime.equals(that.fromCreatedDateTime) :
            that.fromCreatedDateTime != null)
            return false;
        return toCreatedDateTime != null ?
            toCreatedDateTime.equals(that.toCreatedDateTime) :
            that.toCreatedDateTime == null;
    }

    @Override public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (username != null ? username.hashCode() : 0);
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + (jdbcUrl != null ? jdbcUrl.hashCode() : 0);
        result = 31 * result + (jdbcDriver != null ? jdbcDriver.hashCode() : 0);
        result = 31 * result + (parentTable != null ? parentTable.hashCode() : 0);
        result = 31 * result + (childTable != null ? childTable.hashCode() : 0);
        result = 31 * result + (exportDir != null ? exportDir.hashCode() : 0);
        result = 31 * result + (sqlFileName != null ? sqlFileName.hashCode() : 0);
        result = 31 * result + (createTableIfNotExisits != null ?
            createTableIfNotExisits.hashCode() :
            0);
        result = 31 * result + (fromCreatedDateTime != null ? fromCreatedDateTime.hashCode() : 0);
        result = 31 * result + (toCreatedDateTime != null ? toCreatedDateTime.hashCode() : 0);
        return result;
    }

    @Override public String toString() {
        return "DbProperties{" + "name='" + name + '\'' + ", username='" + username + '\''
            + ", password='" + password + '\'' + ", jdbcUrl='" + jdbcUrl + '\'' + ", jdbcDriver='"
            + jdbcDriver + '\'' + ", parentTable='" + parentTable + '\'' + ", childTable='"
            + childTable + '\'' + ", exportDir='" + exportDir + '\'' + ", sqlFileName='"
            + sqlFileName + '\'' + ", createTableIfNotExisits=" + createTableIfNotExisits
            + ", fromCreatedDateTime='" + fromCreatedDateTime + '\'' + ", toCreatedDateTime='"
            + toCreatedDateTime + '\'' + '}';
    }
}
