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
...
if(指定库名 &&  runSQlOnFile && 不是临时表 && isSqlOnFile ){
u  //后续处理，ResolveDataSource
}
...

def isSqlOnFile(UnresolvedRelation):Boolean=>{
判断table name是否是一个路径类型。。//
db_name 是否是 一个格式类型，such as parquet
    
}
```



## isRunSQLOnFile

首先，这里判断确定是runSqlOnFile之后，就交给resolveDataSource类来resolve这个unresolvedRelation。

```scala
class ResolveDataSource(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u: UnresolvedRelation if u.tableIdentifier.database.isDefined =>
      try {
        val dataSource = DataSource(
          sparkSession,
          paths = u.tableIdentifier.table :: Nil,
          className = u.tableIdentifier.database.get)

        val notSupportDirectQuery = try {
          !classOf[FileFormat].isAssignableFrom(dataSource.providingClass)
        } catch {
          case NonFatal(e) => false
        }
        if (notSupportDirectQuery) {
          throw new AnalysisException("Unsupported data source type for direct query on files: " +
            s"${u.tableIdentifier.database.get}")
        }
        val plan = LogicalRelation(dataSource.resolveRelation())
        u.alias.map(a => SubqueryAlias(u.alias.get, plan, None)).getOrElse(plan)
      } catch {
        case e: ClassNotFoundException => u
        case e: Exception =>
          // the provider is valid, but failed to create a logical plan
          u.failAnalysis(e.getMessage)
      }
  }
}
```

这个类很简单，就是通过dbname作为DataSource的class，然后tableName::Nil作为paths，构建一个DataSource。

通过观察DataSource 类。

```
* @param paths A list of file system paths that hold data.  These will be globbed before and
*              qualified. This option only works when reading from a [[FileFormat]].
```

这里，paths这个参数只有当DataSource从FileFormat读取时，才会生效，因此runSqlOnFile这里不会从jdbc读取数据，这就不用判断jdbc格式下的tablename问题。

而在fileFormat下，path必定是以"/"开头，然后对支持的className(dbname)进行判定，甚至不用判定className，就能把这块逻辑放到resolveDataSource那里继续处理。

