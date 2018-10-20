## Spark CBO使用手册

CBO 是cost based optimization的缩写。

在spark sql中使用CBO功能，首先要打开cbo参数开关，然后，使用analyzeTable命令来收集表的一些基本信息，才能在app运行过程中使用CBO。

### 参数

| 参数                                     | 默认值 | 说明                                                         |
| ---------------------------------------- | ------ | ------------------------------------------------------------ |
| spark.sql.cbo.enabled                    | false  | Enables CBO for estimation of plan statistics when set true. |
| spark.sql.cbo.joinReorder.enabled        | false  | Enables join reorder in CBO.                                 |
| spark.sql.cbo.joinReorder.dp.star.filter | false  | Applies star-join filter heuristics to cost based join enumeration. |
| spark.sql.cbo.joinReorder.dp.threshold   | 12     | The maximum number of joined nodes allowed in the dynamic programming algorithm. |
| spark.sql.cbo.starSchemaDetection        | false  | When true, it enables join reordering based on star schema detection. |

要使用spark sql的cbo功能，需要将 spark.sql.cbo.enabled设置为true，默认为false。

spark.sql.cbo.joinReorder.enabled默认为false，建议设置为true，开启之后可以基于代价对多个的join，进行重排序，寻求更好的执行方案。

spark.sql.cbo.joinReorder.dp.star.filter 默认为false，建议设置为true。

spark.sql.cbo.joinReorder.dp.threshold  代表在joinReorder过程中最大支持的表的数量，默认是12，如果在spark sql中有更多的表一起join，比如20个，那就建议把该值设置为20或者更大。

spark.sql.cbo.starSchemaDetection 这个参数默认为false，这个参数很奇怪，加了cbo前缀，却只有在spark.sql.cbo.enabled=false时才会触发，因此此处对这个参数持保留态度。

另外再提一点，本来 spark 选择 join种类，比如broadcastJoin， hashShuffledJoin和sortMergeJoin也是要加入到cbo中来的，不过最终是作为一种基本策略来使用，也就是有没有开启CBO都会进行JoinSelection，但是不同的是，是否开启CBO会影响到spark 对表信息估计的准确度，因此是否开启CBO也最终影响到join种类的选择。

### 命令

spark sql cbo 简单的来说就是基于代价来进行优化，在spark plan形成的一个Tree当中，spark对每一个treeNode进行代价的估测。

代价估测，说白了就相当于一个函数计算，必须要有一个输入。

而表的基本信息就是这个输入，因此使用CBO必须要使用命令先对表的信息进行收集。

```sql
ANALYZE TABLE db_name.table_name COMPUTE STATISTICS
```

上面的命令是对标的基本信息进行收集，例如表的大小(sizeInbytes)，表的行数(rowCount).

```sql
ANALYZE TABLE db_name.table_name COMPUTE STATISTICS FOR COLUMNS column-name1, column-name2, ….
```

这条命令就更加详细了，不仅收集了表的基本信息(sizeInBytes，rowCount)，还会对列的基本信息进行收集(最大值，最小值，空值数量等等)。

这些列的信息对于CBO来说是很有用的，因此在CBO过程中，最好是使用下面的命令，收集更详细的信息。当然，下面的命令收集更多的信息，会消耗相对长的时间，但这对于生产环境下可能进行的多次查询来说是值得的。



在开启cbo之后，我没查到显示如何打印出一个sql语句带有Statistics信息的plan计划的命令。

因此使用下面的代码来代替这个功能。

```scala
      println(spark.sql(sqlStr).queryExecution.stringWithStats)
```



### 附录

下面这个程序可以收集一个database中所有表的详细信息.

```scala
import org.apache.spark.sql.SparkSession

object analyzeTableColumns {
  def main(args:Array[String])={
    require(args.length>0," dataBaseName")
    val dataBase=args(0)
    val spark= SparkSession.builder()
      .appName("analyzeTable"+dataBase)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("use "+dataBase).show()
    val tables= spark.sql("show tables").rdd.collect()

    for(tableArr<- tables){
      //这是一个数组，长度为3，下标为1的是tableName
      val table=tableArr(1)
      val columns= spark.sql("show columns from "+table).rdd.collect()
      // columns的每个元素是一个长度为1的数组
      val colStr=columns.map(arr=>arr(0)).mkString(",")

      val sqlString=" analyze table "+table+"\tCOMPUTE STATISTICS FOR COLUMNS\t"+colStr
      println(sqlString)
      spark.sql(sqlString).show()
    }
    spark.stop()
  }
}
```



