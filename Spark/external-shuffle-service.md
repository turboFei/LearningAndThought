#Spark External Shuffle Service

## 前言

最近公司的spark平台遇到了一个问题。

错误：Unable to create executor due to Unable to register with external shuffle server due to : java.util.concurrent.TimeoutException: Timeout waiting for task

 由于公司用的版本问题，没有紧跟社区更新，发现公司版本里面的代码里`spark.shuffle.registration.timeout` 和`spark.shuffle.registration.maxAttempts`  这两个参数是hardcode，分别为5000ms和3.

```scala
  /**
   * Registers this executor with an external shuffle server. This registration is required to
   * inform the shuffle server about where and how we store our shuffle files.
   *
   * @param host Host of shuffle server.
   * @param port Port of shuffle server.
   * @param execId This Executor's id.
   * @param executorInfo Contains all info necessary for the service to find our shuffle files.
   */
  public void registerWithShuffleServer(
      String host,
      int port,
      String execId,
      ExecutorShuffleInfo executorInfo) throws IOException, InterruptedException {
    checkInit();
    try (TransportClient client = clientFactory.createUnmanagedClient(host, port)) {
      ByteBuffer registerMessage = new RegisterExecutor(appId, execId, executorInfo).toByteBuffer();
      client.sendRpcSync(registerMessage, 5000);
    }
  }
```

此处为`ExternalShuffleClient`中的代码。

在合了[社区PR](https://github.com/apache/spark/pull/18092/commits/80e9ad9e02fbfd24bbd6d97e03b1bdf01e4c922c)之后添加了这两个参数，但是发现之前自己对 spark external shuffle service的了解还不够，因此写这篇文章。

## What is external shuffle service?

首先，什么是外部shuffle服务。 

在工作之前，我没有使用过spark on yarn，都是在standalone模式下跑实验。所以之前没有注意到External shuffle service。

那首先聊一下shuffle service。 shuffle分为两部分，shuffle write和shuffle read，在write端，对每个task的数据，按照key值进行hash，得到新的partitionId，然后将这些数据写到一个partitionFile里面，在paritionFile里面的数据是partitionId有序的，外加会生成一个索引，索引每个partitionFile对应偏移量和长度。

而shuffle read 端就是从这些partitionFile里面拉取相应partitionId的数据，注意是拉取所有partitionFile的相应部分。

在shuffle write阶段对应的是shuffle service，管理生成的partitionFiel，而shuffle read 端对应的是 shuffle client，拉取文件的相应部分。

因此 external shuffle service 就是一个外部代理的shuffle service。 那么为什么需要外部shuffle service呢？

## Why need external shuffle service?

Spark系统在运行含shuffle过程的应用时，Executor进程除了运行task，还要负责写shuffle 数据，给其他Executor提供shuffle数据。当Executor进程任务过重，导致GC而不能为其 他Executor提供shuffle数据时，会影响任务运行。

因此，spark提供了external shuffle service这个接口，常见的就是spark on yarn中的，YarnShuffleService。这样，在yarn的nodemanager中会常驻一个externalShuffleService服务进程来为。

这个服务管理者shuffleMapTasks output files,  这些文件可一直供所有的executor使用。因此，有了这个外部shuffle服务，即使一些executor被killed掉或者回收，shuffle output files依然保留，我们可以看下相应的参数。

## How it works？

与外部shuffle service对应的参数有以下几个。

| `spark.shuffle.service.enabled`      | false | Enables the external shuffle service. This service preserves the shuffle files written by executors so the executors can be safely removed. This must be enabled if `spark.dynamicAllocation.enabled` is "true". The external shuffle service must be set up in order to enable it. See[dynamic allocation configuration and setup documentation](http://spark.apache.org/docs/latest/job-scheduling.html#configuration-and-setup) for more information. |
| ------------------------------------ | ----- | ------------------------------------------------------------ |
| `spark.shuffle.service.port`	| 7337 | 	Port on which the external shuffle service will run. |
| `spark.shuffle.registration.timeout` | 5000  | Timeout in milliseconds for registration to the external shuffle service. |
| `spark.shuffle.registration.maxAttempts` | 3    | When we fail to register to the external shuffle service, we will retry for maxAttempts times. |

第一个参数是打开外部服务，这里看到描述里面写当打开动态分配时，必须设置为true，是为了让外部shuffle service管理shuffle output files，方便释放闲置的executor。

第二个参数是设置shuffle 服务的端口。

后面两个参数，就是注册超时时长与重试次数，在 shuffle需要传输大量数据时，shuffle service比较繁忙，回复这些注册信息的时延较高，因此可能会发生注册失败错误，此时要将这两个参数调大。

在spark on yarn中，会设置以下参数。

```shell
<property>

<name>yarn.nodemanager.aux-services</name>

<value>spark_shuffle</value>

</property>

<property>

<name>yarn.nodemanager.aux-services.spark_shuffle.class</name>

<value>org.apache.spark.network.yarn.YarnShuffleService</value>

</property>

<property>

<name>spark.shuffle.service.port</name>

<value>7337</value>

</property>

```



## Reference 

[Spark Configuration](http://spark.apache.org/docs/latest/configuration.html)

[External Shuffle Service](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-ExternalShuffleService.html)

