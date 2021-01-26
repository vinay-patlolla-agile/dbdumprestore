package com.utility.dbdumprestore;

import com.utility.dbdumprestore.model.DbImportProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.init.ScriptException;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

@Component
public class DbStatementsExecute {

    private Logger logger = LoggerFactory.getLogger(DbStatementsExecute.class);

    private final DbImportProperties dbProperties;

    private final Utility utility;


    public DbStatementsExecute(DbImportProperties dbProperties, Utility utility){
        this.dbProperties = dbProperties;
        this.utility = utility;
    }

    /**
     *
     * @return bool
     * @throws SQLException exception
     * @throws ClassNotFoundException exception
     */
    public boolean importDatabase() throws SQLException, ClassNotFoundException, IOException {

        if(!utility.isValidateProperties()) {
            logger.error("Invalid config properties: The config properties is missing important parameters: DB_NAME, DB_USERNAME and DB_PASSWORD");
            return false;
        }

        //connect to the database
        Connection connection = utility.getImportDbConnection();
        Statement stmt = connection.createStatement();
        try {
            logger.debug("Running the sql file {} ",dbProperties.getSqlFile());
            ScriptUtils.executeSqlScript(connection,new FileSystemResource(new File( dbProperties.getSqlFile())));
        }catch (ScriptException se){
            logger.error("Error executing the sql file {} ",dbProperties.getSqlFile());
        }
        logger.debug("Sql file {} has been processed successfully ",dbProperties.getSqlFile());
        stmt.close();
        connection.close();
        return true;
    }
}
