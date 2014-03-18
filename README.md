solr-mongodb-dih
================
This is a repository for MongoDB-JDBC-Solr integration. Its based on Mongo-JDBC Driver, and it enables you to use DataImportHandler of Apache Solr with this library.

The only limitation it has is, it does not allow select * from queries. You can use it simply just like any other JDBC. We have provided sample DIH configuration file in experiments folder for your reference.

### Supported
 - SELECT
   - field selector
   - order by
 - INSERT
 - UPDATE
   - basics
 - DROP

- In a limited fashion, it supported call to execute(), with access to ResultSetMetadata
