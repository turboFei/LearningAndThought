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