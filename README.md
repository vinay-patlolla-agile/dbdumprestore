# dbdumprestore
DBDumpRestore is a utility for taking the dump for any two related tables by specifying the table
names and common column between them.

The output of the dump is a SQL file with the insert statements per each transaction.

Example:
```roomsql
--
-- start table insert : customer_transaction
--

--
-- start table insert : related_orders
--
INSERT INTO `customer_transaction`(`customer_transaction_id`, `customer_transaction_ref`, `status_id`, `customer_id`, `company_id`, `order_type_id`, `tracking_number`, `created_ts`, `updated_ts`, `created_by`, `updated_by`) VALUES 
('1', '123', 1, 111, 222, 333, '1234', '2021-01-18 22:21:51', '2021-01-18 22:21:51', 'vinay', 'vinay');
--
INSERT INTO `related_orders`(`id`, `customer_transaction_id`, `customer_transaction_ref`, `tracking_number`, `order_status`, `company_id`, `confirmation_number`, `status_description`, `created_ts`, `updated_ts`, `created_by`, `updated_by`) VALUES 
(1, '1', '123', '1234', 1, 1, '1', 'done', '2021-01-18 22:22:43', '2021-01-18 22:23:16', 'vinay');
--

--
-- end table insert : customer_transaction
--

--
-- end table insert : related_orders
--

/*!40000 ALTER TABLE `customer_transaction` ENABLE KEYS */;

/*!40000 ALTER TABLE `related_orders` ENABLE KEYS */;

```


##Usage

Since it is maven spring boot application,create a runnable jar using the below command

```maven
mvn clean install OR mvn clean package
```

The above command creates a runnable jar inside the target folder with  the following name 

```java
dbdumprestore-0.0.1-SNAPSHOT.jar
```

To make the Db values configurable, this application is utilizing the application.properties file which can be outside the jar as well.
The default properties file can be found inside the resources folder.
Move it to your desired folder and use the below command to run the jar.


```java
java -jar -Dspring.config.location=path-to-application.properties dbdumprestore-0.0.1-SNAPSHOT.jar
```

The following are the properties required/optional to run this jar

```text
#Name of the database
db.name=sqldump
#Username of the database
db.username=root
#Password for the database
db.password=root
#Complete jdbc url of the connecting database server
db.jdbcurl=
#Driver required to connect to database
db.jdbcdriver=com.mysql.cj.jdbc.Driver
#The parent table that you want script to query to
db.parenttable=customer_transaction
#The child table that you want script to query to usingthe related column
db.childtable=related_orders
db.relatedcolumn=customer_transaction_ref
db.exportdir=/home/new/dump
#The name of the dump file(Script creates a file name with current timestamp if nothing is provided)
db.sqlfilename=customertransaction_relatedorders
# On true,the script generates the Create table queries as well,default to always false
db.createtableifnotexisits=false
```

