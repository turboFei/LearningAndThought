 这是一个ppt，带视频的，讲解Facebook的shuffle system

https://databricks.com/session/sos-optimizing-shuffle-i-o

相关jira： https://issues.apache.org/jira/browse/SPARK-19659

应该是一个中间件。解决小文件问题，automatic merge。



这个会对spark系统代码侵入么？

首先在driver有一个 shuffle scheduler，这个的作用是相当于master，然后对各个节点上的shuffle service进行调度，告诉他们是否需要进行merge。如果需要merge，则需要shuffle service进行merge操作。



因此这个shuffle merge scheduler 需要嵌入到driver中，需要了解到shuffleMapStatus，因为MapStatus可以得到shuffle文件大小。但是如何合并呢？

![](../../imgs/plan/plan-shufflesystem.png)

这里，可以看到在合并之后，partitionedIndexFile的layout没有变化，还是按照partitionedkey进行划分。如果是有序呢？还是要保证有序吧，这就需要二路归并，还是直接合并呢？还有如果是在map端进行了聚合呢？有影响么？

其实mapsideCombine之后，直接合并是没影响的。那么进行了sort之后呢？难道sort之后直接合并会有问题？我们这里的shuffle system需要进行排序这种耗费内存的操作么？如果是需要map端sort而我们又没有在shuffle合并时进行sort会怎么样？

首先设置一个partitionedFile到底需要多大才比较合适。比如要256m，那么会对小的mapOutput合并。

这是解决了小文件的问题。

但是数据倾斜呢？  数据倾斜在ae中是如何解决的呢？



