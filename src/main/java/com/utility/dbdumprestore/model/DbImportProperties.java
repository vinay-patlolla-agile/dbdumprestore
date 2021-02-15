package com.utility.dbdumprestore.model;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author vinay
 */
@Configuration
@ConfigurationProperties(prefix = "db.import")
public class DbImportProperties {

        private String dbName;
        private String username;
        private String password;
        private String jdbcUrl;
        private String jdbcDriver;

        private String filesFromDirectory;

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

        public String getFilesFromDirectory() {
        return filesFromDirectory;
    }

        public void setFilesFromDirectory(String filesFromDirectory) {
        this.filesFromDirectory = filesFromDirectory;
    }

}
