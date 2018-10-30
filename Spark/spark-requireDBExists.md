## 有两个requireDBExists函数，有什么区别

SessionCatalog和ExternalCatalog 里面各有一个requireDBExists函数，有什么区别？

SessionCatalog在hive环境下中最终调用 HiveExternalCatalog下的 databaseExists函数。

然后externalCatalog在hive环境下最终也是调用HiveExternalCatalog下的 databaseExists函数。

所以两个在hive环境下最终调用是一样的。



## requireDBExists

alterDatabase

createTable

dropTable

altertable

loadTable

listTables

create functions

function exists

list functions 



requireDBExists函数会需要用户对db所在的hdfs目录具有execute权限。但这不符合权限控制的要求。

比如我具有这个表的写权限，但是不一定有execute权限。

同样的表，hive可以查询，而spark不行，就是因为spark 多了requieDBExists。

#  去掉会怎样

如果不进行requireDBExists验证，

在接下来进行tableExists或者requireTableExists时如果db不存在，会抛异常。

getDatabase同样会需要对db所在hdfs目录的execute权限。



也就是说除了alterDatabase依然会有权限影响。





## 测试

select 

createTable

dropTable

altertable

loadTable

***listTables***

create functions

function exists

***list* *functions*** 

### 语句

- select 权限

select * from wb_db.wb_tblas;

- create 权限

create table wb_db.test_create_table as select * from test0;

- create和drop权限

create table wb_tb.test_drop_table as select * from test0;

drop table  wb_tb.test_drop_table;

- alter 权限  - 使用ranger 授予 test_create_table 的alter权限

  ALTER TABLE wb_db.test_create_table SET TBLPROPERTIES ("comment" = "this is a alter operation");

- load table-update 权限

create table 

LOAD DATA LOCAL INPATH '/tmp/student.txt' INTO TABLE student;==

-  list table 应该是需要use database的,所以这个不用管



- create function
- list function- 是否同 list tables，同样需要use database
- drop function

- load partition
- create partition 
- drop partition
- rename table
- alter paritition
- rename partition
- 



所以list tables和 list functions，需要use database，而use database是必须要有database的execute权限，因此这里是没有问题的。但是，由于前面use database做过验证，所以这里去掉也是没有问题的。



所有这里考虑的都是 sessionCaltLog-> hiveMetaStore的情况，

那么对于inMemoryCatalog来说，去掉这个怎么办呢？没有问题