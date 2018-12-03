## 前言

spark shuffle reader是读取shuffle write阶段的数据。如果有m个mapper，r个reducer。那么每个reducer都会读取这m个mapper对应partitionedIndexFile中的数据，所以会造成m*r次fetch操作。

那么这些操作详细过程是什么呢？



## localFetch and remoteFetch

拉取数据分为两种，一种是拉取当前executor节点上的数据，另外一个是拉取远程的数据，local就不需要进行网络传输，但是remote需要。

首先就是要将这些fetch分为local和remote，这是通过对比executorId来实现。

