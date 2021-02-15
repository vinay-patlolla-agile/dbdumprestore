package com.utility.dbdumprestore;

import com.utility.dbdumprestore.model.DbImportProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@Component
public class DbStatementsExecute {

    private Logger logger = LoggerFactory.getLogger(DbStatementsExecute.class);

    private final DbImportProperties dbProperties;

    private final Utility utility;

    private Connection connection;
    private Statement stmt;
    List<String> erroredFiles=new ArrayList<>();


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
    public boolean importDatabase() throws Exception {

        if(!utility.isValidateDbImportProperties()) {
            logger.error("Invalid config properties: The config properties is missing important parameters: DB_NAME, DB_USERNAME and DB_PASSWORD");
            return false;
        }

        connection = utility.getImportDbConnection();
        stmt = connection.createStatement();

        try (Stream<Path> paths = Files.walk(Paths.get(dbProperties.getFilesFromDirectory()))) {
            paths
                .filter(Files::isRegularFile)
                .forEach(this::executeSql);
        }finally {
            stmt.close();
            connection.close();
            if(!erroredFiles.isEmpty()){
                logger.warn("Files errored out {} ",erroredFiles);
            }
        }
        return true;
    }

    //@Transactional(rollbackFor = ScriptStatementFailedException.class)
    private boolean executeSql(Path path){
        ImportScriptRunner scriptRunner=new ImportScriptRunner(connection,false,true);
        try {
            logger.info("Running the script file {}",path.toFile());
            scriptRunner.runScript(new FileReader(path.toFile()));
            logger.info("File  {} has been successfully executed",path.toFile());
        } catch (IOException e) {
            logger.error("Error executing the script {} ",path.toFile());
            erroredFiles.add(path.toFile().getPath());
            return false;
        } catch (SQLException e) {
            logger.error("Error executing the script {} ",path.toFile());
            erroredFiles.add(path.toFile().getPath());
            return false;
        }
        return true;
    }
}
