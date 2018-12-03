## Spark yarnShuffleService

spark在yarn上运行时，可以设置spark.shuffle.service.enable为true，来托管各个executor的shuffle，这样就可以使得executor在运行完毕之后可以回收，也使得在executor崩溃时不会丢失shuffle的数据。



所以yarnshuffleService的瓶颈在哪呢？

目前知道的是，在每个nodeManager里面都有一个进程为externalShuffleService。也就是说，我一个应用会对应多个nm上的ess，那么这些ess会存在master和slaves么？

我一个block该怎么去拉取呢？看源码。

还有就是拉取是一个怎样的过程呢？是如何占用资源的？

