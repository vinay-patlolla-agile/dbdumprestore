package com.utility.dbdumprestore;

import com.utility.dbdumprestore.model.DbProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;

@SpringBootApplication
public class DbdumprestoreApplication {

	private Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) {
		ConfigurableApplicationContext configurableApplicationContext = SpringApplication.run(DbdumprestoreApplication.class, args);

		DbProperties dbProperties = configurableApplicationContext.getBean(DbProperties.class);
		Utility utility = configurableApplicationContext.getBean(Utility.class);
		DbExport dbExport=new DbExport(dbProperties, utility);
		DbDelete dbDelete=new DbDelete(dbProperties, utility);
		Scanner scanner = new Scanner(System.in);
		System.out.println("Please choose an option \n 1.Data backup \n 2.Generate Delete Statements \n Press q to Quit \n");
		String choice = scanner.next();

		switch (choice){
			case "1":
				try {
					dbExport.export();
				} catch (IOException e){
					System.out.println("IO error "+e);
				}catch (ClassNotFoundException cne){
					System.out.println("Class not found error "+cne);
				}catch (SQLException sqle){
					System.out.println("Error exporting "+sqle);
				}
				break;
			case "2":
				try {
					dbDelete.exportDeleteStatements();
				} catch (IOException e){
					System.out.println("IO error "+e);
				}catch (ClassNotFoundException cne){
					System.out.println("Class not found error "+cne);
				}catch (SQLException sqle){
					System.out.println("Error exporting "+sqle);
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
