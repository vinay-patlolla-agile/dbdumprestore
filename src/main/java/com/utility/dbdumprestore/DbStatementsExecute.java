package com.utility.dbdumprestore;

import com.utility.dbdumprestore.model.DbImportProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

   /* private DbStatementsExecute() {
        this.deleteExisting = false;
        this.dropExisting = false;
        this.tables = new ArrayList<>();
    }*/

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
        Connection connection = utility.getConnection();

        Statement stmt = connection.createStatement();

        String sqlString = readSqlFileAsString();
        if(StringUtils.hasLength(sqlString)){
            sqlString = sqlString.trim();
        }

        //disable foreign key check
        stmt.addBatch("SET FOREIGN_KEY_CHECKS = 0");
        stmt.addBatch(sqlString);
        //add enable foreign key check
        stmt.addBatch("SET FOREIGN_KEY_CHECKS = 1");

        //now execute the batch
        long[] result = stmt.executeLargeBatch();

        if(logger.isDebugEnabled())
            logger.debug( result.length + " queries were executed in batches for provided SQL String with the following result : \n" + Arrays.toString(result));

        stmt.close();
        connection.close();

        return true;
    }

    private String readSqlFileAsString() throws IOException {

        File sqlFile = new File( dbProperties.getSqlFile());
        logger.info("SQL File name: " + sqlFile.getAbsolutePath());

        return new String(Files.readAllBytes(sqlFile.toPath()));
    }


}
