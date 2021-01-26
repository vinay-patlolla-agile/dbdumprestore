package com.utility.dbdumprestore.model;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "db.import")
public class DbImportProperties {
    String database;
    String username;
    String password;
    String jdbcUrl;
    String jdbcDriver;
    String sqlFile;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
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

    public String getSqlFile() {
        return sqlFile;
    }

    public void setSqlFile(String sqlFile) {
        this.sqlFile = sqlFile;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DbImportProperties that = (DbImportProperties) o;

        if (database != null ? !database.equals(that.database) : that.database != null)
            return false;
        if (username != null ? !username.equals(that.username) : that.username != null)
            return false;
        if (password != null ? !password.equals(that.password) : that.password != null)
            return false;
        if (jdbcUrl != null ? !jdbcUrl.equals(that.jdbcUrl) : that.jdbcUrl != null)
            return false;
        if (jdbcDriver != null ? !jdbcDriver.equals(that.jdbcDriver) : that.jdbcDriver != null)
            return false;
        return sqlFile != null ? sqlFile.equals(that.sqlFile) : that.sqlFile == null;
    }

    @Override public int hashCode() {
        int result = database != null ? database.hashCode() : 0;
        result = 31 * result + (username != null ? username.hashCode() : 0);
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + (jdbcUrl != null ? jdbcUrl.hashCode() : 0);
        result = 31 * result + (jdbcDriver != null ? jdbcDriver.hashCode() : 0);
        result = 31 * result + (sqlFile != null ? sqlFile.hashCode() : 0);
        return result;
    }

    @Override public String toString() {
        return "DbImportProperties{" + "database='" + database + '\'' + ", username='" + username
            + '\'' + ", password='" + password + '\'' + ", jdbcUrl='" + jdbcUrl + '\''
            + ", jdbcDriver='" + jdbcDriver + '\'' + ", sqlFile='" + sqlFile + '\'' + '}';
    }
}
