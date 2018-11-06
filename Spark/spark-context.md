## Spark context

spark context运行在driver之上。

spark context是用来连接cluster的，所以所有的并行相关操作都与其有关。

而且，在每次进行一些操作，比如 text,parallelize操作都先assertNotStopped。

如果停止了，就报错，并且打印出这个sparkContext的createSite。



driver上面是可以对一个sql应用完成catalyst编译的过程，但是如果需要开始执行，是必须要使用sparkContext的，也就是说，driver进程依然存在，而sparkContext已然停止。

