## 描述

使用情况是在跨项目中，项目a中的表，公开，然后对项目b中的一个用户授权，给读权限。

查询语句：

```
select basic_info.id, goods.supplier_id, goods.goods_id, nvl(goods_uv_pv.pv, 0) as pv, nvl(goods_uv_pv.uv, 0) as uv from kaola_decision_fdm.tb_business_basicinfo basic_info join kaola_decision_fdm.tb_business_mapping mapping on basic_info.id = mapping.businessid join kaola_decision_fdm.tb_goods goods on mapping.kaolabusinessid = goods.supplier_id left join ( select goods_id, sum(pv) as pv, sum(uv) as uv from haitao_dev_log.dws_kl_flw_goods_os_1d where day >= '2018-10-10' group by goods_id ) goods_uv_pv on goods.goods_id = goods_uv_pv.goods_id

```

其中`haitao_dev_log`是一个外部表。这是项目da_haitao中的表。

然后用户是在项目 kaola_decision。

但是却报用户对这个表所在的hdfs路径没有权限。

日志如下：

```
Exception in thread "main" org.apache.spark.sql.AnalysisException: org.apache.hadoop.hive.ql.metadata.HiveException: MetaException(message:java.security.AccessControlException: Permission denied: user=bdms_hzleiting, access=EXECUTE, inode="/user/da_haitao/hive_db/haitao_dev_log.db":da_haitao:hdfs:drwxr-x---
at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.checkAccessAcl(FSPermissionChecker.java:403)
at org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.check(FSPermissionChecker.java:306)

```



## 问题分析



报的错是：

```
Caused by: org.apache.hadoop.hive.ql.metadata.HiveException: MetaException(message:java.security.AccessControlException: Permission denied: user=bdms_hzleiting, access=EXECUTE, inode="/user/da_haitao/hive_db/haitao_dev_log.db":da_haitao:hdfs:drwxr-x---

```

说是用户没有对这个库所在的hdfs的 execute权限。

由于用户在kaola_decision中，而这个表是在 da_haitao项目中公开。

 

在spark中，在analyzer阶段, ResolveRealtion类中，的apply方法中，如果这个是一个unresolvedRelation，如果指定了database，则会判断，这个库是否存在。

```scala
  case u: UnresolvedRelation =>
       val table = u.tableIdentifier
       if (table.database.isDefined && conf.runSQLonFile && !catalog.isTemporaryTable(table) &&
           (!catalog.databaseExists(table.database.get) || !catalog.tableExists(table))) {
          // If the database part is specified, and we support running SQL directly on files, and
          // it's not a temporary view, and the table does not exist, then let's just return the
          // original UnresolvedRelation. It is possible we are matching a query like "select *
          // from parquet.`/path/to/query`". The plan will get resolved later.
          // Note that we are testing (!db_exists || !table_exists) because the catalog throws
          // an exception from tableExists if the database does not exist.
         u
       } 
```

在判断库是否存在，底层会调用HDFS的checkPerscheckPermission.

由于`inode="/user/da_haitao/hive_db/haitao_dev_log.db":da_haitao:hdfs:drwxr-x—`，权限为对分组内可拥有执行权限，所以项目外必然是分组外，也就没有权限。

所以就造成了失败。



**这个dbexists判断，是针对每条 unresolvedRelation都会进行判断的。**



###  去掉有什么影响

首先，这个判断只要是查询中声明了库的名字，都会触发这个dbexist的验证。

分析一下这里的判断逻辑。

```scala
  case u: UnresolvedRelation =>
       val table = u.tableIdentifier
       if (table.database.isDefined && conf.runSQLonFile && !catalog.isTemporaryTable(table) &&
           (!catalog.databaseExists(table.database.get) || !catalog.tableExists(table))) {
         u
       } 
```



首先是判断查询中，有没有指定库名字，然后是判断是否允许**runSqlOnFile**。

>这是一个相当于DataSource的用法，格式是 classType.path
>
>例如parquet./user/hive/parquetData
>
>就相当于从/user/hive/parquetData读取parquet数据，
>
>这个参数是受`spark.sql.runSQLOnFiles`控制，默认为true

之后再判断如果这张表不是临时表，且（它对应的库不存在或者表不存在）,然后就返回原来的unresolvedRelation，后续处理。

**后续怎么处理？**

上述判断之后不处理，直接返回unresolvedRelation，是认为这张表是一个DataSource类型的表，会在

`ResolveDataSource`规则中处理这个表。





### 转换一下这里的判断逻辑

```
if(指定库名 &&  runSQlOnFile && 不是临时表 &&  判断可以runSqlOnFile(表的名字-路径来判断)){
u  //后续处理，ResolveDataSource
}
```



如果这里不判断dbexist和tabexist

后面会出问题么？

