## spark hive元数据不同步

http://jira.netease.com/browse/NESPARK-164?filter=-1

1、orc不同步，测试text，parquet等是否同步

​	text没问题

2、最新版本的spark是否已经解决这个问题，还有最新版本的hive

3、orc建表时，Array[String]问题





create external table wb_db.bd939_orc_sql_4 stored as orc location /user/wangfei3/bd939_orc_sql_4  tblproperties (compression=snappy) as select * from wb_tblas;





create table  



create external table wb_db.bd939_orc_sql_4 stored as orc location "/user/wangfei3/bd939_orc_sql_4"  tblproperties ('compression'='snappy') as select * from wb_tblas;



##  使用dataframe创建表

show create table

spark:

```
CREATE TABLE `test_orc_in` (`host` STRING, `port` INT, `active` INT, `threads_num` INT, `weight` INT) USING orc OPTIONS ( `compression` 'snappy', `serialization.format` '1' )
```



hive

```
CREATE TABLE `test_orc_in`( `host` string, `port` int, `active` int, `threads_num` int, `newweight` int) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' WITH SERDEPROPERTIES ( 'compression'='snappy', 'path'='hdfs://hz-cluster8/user/sandbox/hive_db/azkaban_autotest_db.db/test_orc_in') STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION 'hdfs://hz-cluster8/user/sandbox/hive_db/azkaban_autotest_db.db/test_orc_in' TBLPROPERTIES ( 'last_modified_by'='bdms_wangfei3', 'last_modified_time'='1541561870', 'numFiles'='2', 'spark.sql.sources.provider'='orc', 'spark.sql.sources.schema.numParts'='1', 'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"host\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"port\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"active\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"threads_num\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"weight\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}', 'totalSize'='1160', 'transient_lastDdlTime'='1541561870')
```

貌似是多了 **'spark.sql.sources.provider'='orc'**

## 使用spark sql创建表

spark:

```
CREATE TABLE `test_orc_sql`(`host` string, `port` int, `active` int, `threads_num` int, `newweight` int) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' WITH SERDEPROPERTIES ( 'serialization.format' = '1' ) STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' TBLPROPERTIES ( 'rawDataSize' = '-1', 'numFiles' = '2', 'transient_lastDdlTime' = '1541560880', 'last_modified_time' = '1541560880', 'last_modified_by' = 'bdms_wangfei3', 'compression' = 'snappy', 'totalSize' = '1043', 'numRows' = '-1' )
```

hive:

```
CREATE TABLE `test_orc_sql`( `host` string, `port` int, `active` int, `threads_num` int, `newweight` int) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' LOCATION 'hdfs://hz-cluster8/user/sandbox/hive_db/azkaban_autotest_db.db/test_orc_sql' TBLPROPERTIES ( 'compression'='snappy', 'last_modified_by'='bdms_wangfei3', 'last_modified_time'='1541560880', 'numFiles'='2', 'numRows'='-1', 'rawDataSize'='-1', 'spark.sql.sources.schema.numParts'='1', 'spark.sql.sources.schema.part.0'='{\"type\":\"struct\",\"fields\":[{\"name\":\"host\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"port\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"active\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"threads_num\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"weight\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}', 'totalSize'='1043', 'transient_lastDdlTime'='1541560880')


```

相关JIRA： https://issues.apache.org/jira/browse/SPARK-21841

这个问题spark-2.3.2同样存在。



## DataSourceTable与HiveTable

目前， spark sql针对表有两种方式，一种是DataSource table，另一种是hiveTable

这两种表不兼容。

DataSource table的元数据怕是和hive不兼容，不管你怎么改，这些元数据不会映射。