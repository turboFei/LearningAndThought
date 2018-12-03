[TOC]



##  关系？

在spark中，sparkContext和driver，sparkSession以及executor之间是什么关系呢？



看kyubbi中同一个用户共享sparkContext。

sparkContext好像是在driver上面，而一个driver就是用于调度信息。

那如果比如说一个项目中，多个人使用一个产品账号，那么很多个session都是共用一个context，那driver不会爆掉么。

所以需要知道，driver和context之间的关系，context是否必须要在driver之中，还是说context可以剥离出来。

那么sparkSession又是什么，sparkSession是否包括调度模块呢？应该不包括。

sparkSession是否就只是包含executor这些东西。



## SparkContext

```scala
/**
 * Alternative constructor that allows setting common Spark properties directly
 *
 * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
 * @param appName A name for your application, to display on the cluster web UI.
 * @param sparkHome Location where Spark is installed on cluster nodes.
 * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
 *             system or HDFS, HTTP, HTTPS, or FTP URLs.
 * @param environment Environment variables to set on worker nodes.
 */
def this(
    master: String,
    appName: String,
    sparkHome: String = null,
    jars: Seq[String] = Nil,
    environment: Map[String, String] = Map()) = {
  this(SparkContext.updatedConf(new SparkConf(), master, appName, sparkHome, jars, environment))
}
```

看到SparkContext的构造函数，指定的有master，appName，sparkHome，jars和一些参数。

因此，SparkContext需要一个conf来生成。

其实driver是什么呢？

个人理解，一个应用程序的SparkContext生成语句，就是在driver之上执行，然后那些执行语句通过算子的并行语义在各个executor之上执行。

因此，我认为一个context就对应了一个driver。也就是说，一个用户对应一个driver。



然后sparkSession，是基于context的。但是一个context可以有多个sparkSession。所以调度信息是谁来控制？

如何控制各个sparkSession的调度信息呢？肯定还是这个driver控制。

但是，sparkSession呢？难道是为每次查询都建一个session？

每次beeline查询都要重新建一个session。



