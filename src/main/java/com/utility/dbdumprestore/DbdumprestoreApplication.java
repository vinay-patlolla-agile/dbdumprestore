package com.utility.dbdumprestore;

import com.utility.dbdumprestore.model.DbProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

@SpringBootApplication
public class DbdumprestoreApplication {

	private Logger logger = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) {
		ConfigurableApplicationContext configurableApplicationContext = SpringApplication.run(DbdumprestoreApplication.class, args);

		DbProperties dbProperties = configurableApplicationContext.getBean(DbProperties.class);
		/*Properties props =new Properties();
		props.put("DB_NAME","sqldump");
		props.put("DB_USERNAME","root");
		props.put("DB_PASSWORD","root");
		//props.put("JDBC_CONNECTION_STRING","");
		props.put("JDBC_DRIVER_NAME","com.mysql.cj.jdbc.Driver");
		props.put("PARENT_TABLE","customer_transaction");
		props.put("CHILD_TABLE","related_orders");
		props.put("PRESERVE_GENERATED_SQL_FILE","true");
		props.put("DIR_TO_EXPORT","/home/vinay/works/private/sqldump/DATA");
		props.put("SQL_FILE_NAME","customertransaction_relatedorders");*/
		DbExport dbExport=new DbExport(dbProperties);
		try{
			dbExport.export();
		}catch (IOException e){
			System.out.println("IO error "+e);
		}catch (ClassNotFoundException cne){
			System.out.println("Class not found error "+cne);
		}catch (SQLException sqle){
     System.out.println("Error exporting "+sqle);
		}


	}

}
