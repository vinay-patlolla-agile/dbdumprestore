package com.utility.dbdumprestore;

import com.utility.dbdumprestore.model.DbExportDeleteProperties;
import com.utility.dbdumprestore.model.DbExportInsertProperties;
import com.utility.dbdumprestore.model.DbImportProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.util.Scanner;

@SpringBootApplication
//@EnableTransactionManagement
@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class, HibernateJpaAutoConfiguration.class})
public class DbdumprestoreApplication {

	/*@Bean
	public PlatformTransactionManager transactionManager(DataSource dataSource)
	{
		return new DataSourceTransactionManager(dataSource);
	}*/

	private Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) {
		ConfigurableApplicationContext configurableApplicationContext = SpringApplication.run(DbdumprestoreApplication.class, args);

		DbExportInsertProperties dbExportInsertProperties = configurableApplicationContext.getBean(DbExportInsertProperties.class);
		DbImportProperties dbImportProperties = configurableApplicationContext.getBean(DbImportProperties.class);
		DbExportDeleteProperties dbExportDeleteProperties = configurableApplicationContext.getBean(DbExportDeleteProperties.class);
		Utility utility = configurableApplicationContext.getBean(Utility.class);
		DbExport dbExport=new DbExport(dbExportInsertProperties, utility);
		DbDelete dbDelete=new DbDelete(dbExportDeleteProperties, utility);
		Scanner scanner = new Scanner(System.in);
		System.out.println("Please choose an option \n 1.Data backup \n 2.Generate Delete Statements \n 3.Import \n Press q to Quit \n");
		String choice = scanner.next();

		switch (choice){
			case "1":
				try {
					dbExport.export();
				} catch (Exception e){
					System.out.println("IO error "+e);
				}
				break;
			case "2":
				try {
					dbDelete.exportDeleteStatements();
				} catch (Exception e){
					System.out.println("Error in exporting the delete statements "+e);
				}
				break;
			case "3":
				try {
					DbStatementsExecute dbStatementsExecute=new DbStatementsExecute(dbImportProperties, utility);
					dbStatementsExecute.importDatabase();
				} catch (Exception e){
					System.out.println("IO error "+e);
				}
				break;
			case "q":
				System.out.println("Skipping the execution");
				break;
			default:
				break;
		}
		scanner.close();
	}

}
