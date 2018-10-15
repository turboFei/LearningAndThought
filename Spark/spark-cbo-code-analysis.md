##  Spark CBO 源码分析

CBO是基于Cost来优化plan。

要计算cost就需要统计一些参与计算的表的相关信息，因此spark添加了`Statistics和ColumnStat`类来统计相关信息。

CBO主要是针对join来计算cost,目前spark-2.3 版本中与CBO相关的参数如下：

|  参数| 默认值| 说明|
| ---------------------------------------- | ----- | ------------------------------------------------------------ |
| spark.sql.cbo.enabled                    | false | Enables CBO for estimation of plan statistics when set true. |
| spark.sql.cbo.joinReorder.enabled        | false | Enables join reorder in CBO.                                 |
| spark.sql.cbo.joinReorder.dp.star.filter | false | Applies star-join filter heuristics to cost based join enumeration. |
| spark.sql.cbo.joinReorder.dp.threshold   | 12    | The maximum number of joined nodes allowed in the dynamic programming algorithm. |
| spark.sql.cbo.starSchemaDetection        | false | When true, it enables join reordering based on star schema detection. |

下文按照逻辑顺序分析spark cbo 源码。

## 统计信息类

CBO相关的统计信息类有两个，一个是ColumnStat,代表的是表中列的详细，例如最大值，最小值，空值个数，平均长度，最大长度。另外一个类是Statistics，这个类是对应一个LogicalPlan的统计信息，例如join，aggregate，logicalRelation。

```Scala
case class Statistics(
    sizeInBytes: BigInt,
    rowCount: Option[BigInt] = None,
    attributeStats: AttributeMap[ColumnStat] = AttributeMap(Nil),
    hints: HintInfo = HintInfo()) 

case class ColumnStat(
    distinctCount: BigInt,
    min: Option[Any],
    max: Option[Any],
    nullCount: BigInt,
    avgLen: Long,
    maxLen: Long,
    histogram: Option[Histogram] = None) 
```

如上所示，可以看到ColumnStat表示列的详细信息。

而Statistics，中的sizeInBytes和rowCount就代表这个logicalPlan输出数据的大小和行数，而attributeStats 代表这个logicalPlan涉及到的列的统计信息（一个expressID到列信息的映射），和hints。

对于join来说，它的Statistics里的信息就代表join操作输出的大小，行数以及attributeStats。

对于logicalRelation，它的Statistics代表其对应表中schema相关数据的大小，行数，attributeStats。

`CatalogStatistics`这个类表示存储在外部catalog(例如hive metastore）中的表的信息.

```scala
case class CatalogStatistics(
    sizeInBytes: BigInt,
    rowCount: Option[BigInt] = None,
    colStats: Map[String, ColumnStat] = Map.empty)
```

这些表的信息需要使用 `analyze table`命令来计算，然后存储到catalog里。

每种LogicalPlan计算Statistics的方法是不同的。

对于LogicalRelation来说，它是读取对应表中schema，使用CatalogStatistics类的toPlanStats可以生成Statistics。

```scala
def toPlanStats(planOutput: Seq[Attribute], cboEnabled: Boolean): Statistics = {
  if (cboEnabled && rowCount.isDefined) {
    val attrStats = AttributeMap(planOutput.flatMap(a => colStats.get(a.name).map(a -> _)))
    // Estimate size as number of rows * row size.
    val size = EstimationUtils.getOutputSize(planOutput, rowCount.get, attrStats)
    Statistics(sizeInBytes = size, rowCount = rowCount, attributeStats = attrStats)
  } else {
    // When CBO is disabled or the table doesn't have other statistics, we apply the size-only
    // estimation strategy and only propagate sizeInBytes in statistics.
    Statistics(sizeInBytes = sizeInBytes)
  }
}
```

下面将介绍其他LogicalPlan的Statistics计算。

## Statistics的计算

看LogicalPlanStats类，可以看出，这里，判断cbo是否开启，如果cbo打开，则采用BasicStatsPlanVisitor类来计算相关的Statistics，如果没有cbo，则使用SizeInBytesOnlyStatsPlanVisitor来计算。

从类的名字就可以看出来，只有cbo开启，才会计算rowCount以及attributeStats信息，如果没有cbo,SizeInBytesOnlyStatsPlanVisitor只会计算 size信息。

```scala
trait LogicalPlanStats { self: LogicalPlan =>
  def stats: Statistics = statsCache.getOrElse {
    if (conf.cboEnabled) {
      statsCache = Option(BasicStatsPlanVisitor.visit(self))
    } else {
      statsCache = Option(SizeInBytesOnlyStatsPlanVisitor.visit(self))
    }
    statsCache.get
  }
}
```

其实在BasicStatsPlanVisitor类中对于大部分类型的LogicalPlan都还是调用SizeInBytesOnlyStatsPlanVisitor的方法来计算。

只有针对Aggregate，Join，Filter，Project有另外的计算方法。

这里讲下join操作的Statistics计算过程。



如果没有开启CBO，join操作首先判断是否是 leftAntiJoin或者是LeftSemiJoin，如果是，则把leftChild的sizeInBytes作为计算结果，因为对于leftAntiJoin和leftSemiJoin来说，join之后表的大小是小于leftChild的。而对于其他类型的join，把左右child的sizeInBytes相乘作为join之后的大小，并且关闭掉broadcastHint，因为这些join类型可能造成很大的output。而这种粗糙的代价估计造成的结果就是，对代价估计不准确，如果该join是可以进行broadcastjoin，也可能由于粗糙的代价估计变得不可进行。

如果开启了CBO，对于join操作就不止计算sizeInBytes，还需要计算rowCount，AttributeStats。

代码如下，首先是判断join类型，如果是 inner,cross,leftOuter,RightOuter,FullOuter中的一种，则使用estimateInnerOuterJoin方法。

```scala
def estimate: Option[Statistics] = {
  join.joinType match {
    case Inner | Cross | LeftOuter | RightOuter | FullOuter =>
      estimateInnerOuterJoin()
    case LeftSemi | LeftAnti =>
      estimateLeftSemiAntiJoin()
    case _ =>
      logDebug(s"[CBO] Unsupported join type: ${join.joinType}")
      None
  }
```

这里只针对针对estimateInnerOuterJoin方法，用语言描述一下：

> 如果是equiJoin:
>
> > 1、首先估算被equi条件选择的记录条数,即等于innerJoin选择的条数，命名为numInnerJoinedRows；以及这些equi涉及的key在join之后的stats。
> >
> > >  即在join中，存在类似 a.co1=b.co1, a.co2=b.co2 这些类似条件，现在是估计满足这些相等条件的记录条数。
> > >
> > > 使用的公式是： T(A J B) = T(A) * T(B) / max(V(A.ki), V(B.ki)).
> >
> > 2、 预估得到结果的行数。
> >
> > > 因为即使满足这些相等条件，也不会只输出这些满足条件的记录。
> > >
> > > 如果是leftOuterJoin，则会对左边表中所有记录都会输出，不管右边匹配是否为空。
> > >
> > > 因此，对于leftOuterJoin来说，输出的记录条数等于max(左边表条数，numInnerJoinedRows)。
> > >
> > > 同样还有rightOuterJoin,输出记录条数=max(右边表条数，numInnerJoinedRows)。
> > >
> > > 对于全连接，输出记录条数=max(左边表条数，numInnerJoinedRows)+max(右边表条数，numInnerJoinedRows)-numInnerJoinedRows。即类似于A与B的并集-A与B的交集。
> >
> > 3、然后是根据前面的计算结果更新Statistics，包括attributeStats。
>
> 如果不是equiJoin：
>
> > 则按照笛卡尔积来计算，输出行数为两个表行数的乘积
> >

## 拿到数据之后怎么用

这些Statistics的结果，会怎么运用到LogicalPlan的优化中？

#### JoinSelection

在SparkPlanner类中，有几个优化策略会对LogicalPlan进行优化。

```scala
class SparkPlanner(
    val sparkContext: SparkContext,
    val conf: SQLConf,
    val experimentalMethods: ExperimentalMethods)
  extends SparkStrategies {
	...
  override def strategies: Seq[Strategy] =
    experimentalMethods.extraStrategies ++
      extraPlanningStrategies ++ (
      DataSourceV2Strategy ::
      FileSourceStrategy ::
      DataSourceStrategy(conf) ::
      SpecialLimits ::
      Aggregation ::
      JoinSelection ::
      InMemoryScans ::
      BasicOperators :: Nil)
      ...
  }
```

里面有一个JoinSelection方法，这个方法是主要是用来判断是否可以使用broadcastjoin，然后决定是使用broadcastJoin，还是shuffledHashJoin还是sortMergeJoin。

broadcastjoin可以避免shuffle，如果使用得当，可以提升程序的性能。`这是针对一个大表和一个极小表`在spark中有一个参数是，`spark.sql.autoBroadcastJoinThreshold`，这个参数是一个数字，单位字节，代表如果一个表的szie小于这个数值，就可以进行broadcastjoin。但是这里只使用size作为估计是不准确的，还应该使用rowCount作为参考，因为在join中，join的结果是与两个表的条数强相关，只使用size做判断是不准确的。

在spark中，有BroadCastHint，前面也提到过，如果没有开启cbo，那么如果判断join类型是非leftAntiJoin和leftSemiJoin，则会觉得join之后的大小无法估测，可能会爆炸式增长，因此会关掉BroadcastHint。

对于shuffledHashJoin，`这是针对一个大表和一个小表（判断标准为a.stats.sizeInBytes * 3 <= b.stats.sizeInBytes)`，简单描述一下过程就是两个表A和B，首先，选择一个表进行shuffle write操作，即针对每个分区，按照key的hash值进行排序，将相同hash值的key放在一起，形成一个partitionFile，然后在read端拉取write端所有相应key的数据，作为localhashMap和另外一个标的分区进行join。

这里也使用stats进行判断，如果`plan.stats.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions`，则判断该表的size可以满足每个分区构建localhashMap的可能，可以看到这里也是以`autoBroadcastJoinThreshold`作为衡量标准。

如果是两张大表，则需要使用sortmergeJoin，类似于先排序，即按照keypair排序，然后进行归并。



这些join selection的操作，不管是否开启CBO都会进行。但是和CBO相关的是，这些数据的统计是和CBO有关，前面提过，如果开启CBO则使用BasicStatsPlanVisitor来进行统计。

上述的这些估测，都是基于size信息。但是即使是基于size信息，如果没有开启cbo，这些信息也是粗糙的，没有CBO那种更细致的估计，因此可能会造成Join种类选择不合适。

上述的判断，很多是基于`spark.sql.autoBroadcastJoinThreshold`，因此在运行环境中，一定要结合集群环境设置合适的值。

而且，在join中，也应该基于rowCount来判断join的种类。

#### CostBasedJoinReorder

这是一个使用plan的stats信息，来选择合适的join顺序的类。

首先看下对cost的定义。cost是有一个基数，是rowCount，然后一个sizeInBytes。

```scala
/**
 * This class defines the cost model for a plan.
 * @param card Cardinality (number of rows).
 * @param size Size in bytes.
 */
case class Cost(card: BigInt, size: BigInt) {
  def +(other: Cost): Cost = Cost(this.card + other.card, this.size + other.size)
}
```

而判断cost的方法是：

A: Cost(ac,as)  B: Cost(bc,bs)

如果

(ac/bc)\*joinReorderCardWeight +(as/bs)\*(1-joinReorderCardWeight)<1，

则认为A比B好。`spark.sql.cbo.joinReorder.card.weight`默认为0.7。代码如下：

```scala
def betterThan(other: JoinPlan, conf: SQLConf): Boolean = {
  if (other.planCost.card == 0 || other.planCost.size == 0) {
    false
  } else {
    val relativeRows = BigDecimal(this.planCost.card) / BigDecimal(other.planCost.card)
    val relativeSize = BigDecimal(this.planCost.size) / BigDecimal(other.planCost.size)
    relativeRows * conf.joinReorderCardWeight +
      relativeSize * (1 - conf.joinReorderCardWeight) < 1
  }
}
```



joinRecorder是使用一个动态规划来进行选择合适的join顺序。