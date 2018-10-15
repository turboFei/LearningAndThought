## Spark History  server测试报告

安装包地址为：http://repo.bdms.netease.com/dev_packages/common/spark/spark-2.3.2-bin-ne-0.0.0.tgz

md5文件： http://repo.bdms.netease.com/dev_packages/common/spark/spark-2.3.2-bin-ne-0.0.0.tgz.md5

md5:  ae924a4ddba28319cc39e108271293e5  spark-2.3.2-bin-ne-0.0.0.tgz

##  1、功能性测试

在测试集群上hzabj-ambari-dev{7-12}.server.163.org 进行测试。

使用新版spark History server直接读取 hdfs://spark2-history 中的application log。

可以正常读取，并且信息显示正常。



## 2、兼容性测试

测试集群上面的spark社区版本为2.1.2，除此之外：

针对spark-1.6.3版本的event log进行兼容性测试。

测试版本在官网下载安装。

https://archive.apache.org/dist/spark/spark-1.6.3/spark-1.6.3-bin-hadoop2.6.tgz

通过spark-1.6.3 运行一些benchMark的spark application，然后读取这些应用的日志，显示正常。

## 3、 线上测试

在线下测试之后，在`spark1.lt.163.org`节点上进行手动部署。

将安装包解压，在conf/spark-env.sh中配置。

**spark-env.sh**中所有配置选项都可从老版本history server配置文件拷贝，注意调大SPARK_DAEMON_MEMORY至40G。

```
# JVM内存设置40G，可按需调大
export SPARK_DAEMON_MEMORY=40960m
# kerboers配置项为/home/hadoop/krb5/krb5.conf 
export SPARK_DAEMON_JAVA_OPTS="-server -XX:+UseParNewGC  -XX:ParallelGCThreads=30  -XX:MaxTenuringThreshold=10  -XX:TargetSurvivorRatio=70  -XX:+UseConcMarkSweepGC -XX:+CMSPermGenSweepingEnabled   -XX:+CMSConcurrentMTEnabled  -XX:ParallelCMSThreads=30  -XX:+UseCMSInitiatingOccupancyOnly  -XX:+CMSClassUnloadingEnabled  -XX:+DisableExplicitGC  -XX:CMSInitiatingOccupancyFraction=70  -XX:+CMSParallelRemarkEnabled  -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+UseCMSCompactAtFullCollection  -XX:CMSFullGCsBeforeCompaction=1  -verbose:gc  -XX:+PrintGCDetails  -XX:+PrintGCDateStamps  -XX:GCLogFileSize=512M  -Xloggc:/home/hadoop/logs/gc-sparkhs.log -Djava.security.krb5.conf=/home/hadoop/krb5/krb5.conf ${SPARK_DAEMON_JAVA_OPTS}"
export JAVA_HOME=/usr/jdk64/jdk1.8.0_77
export HADOOP_HOME=${HADOOP_HOME:-/usr/ndp/current/mapreduce_client}
export HADOOP_CONF_DIR=/home/hadoop/hadoop_conf
export LD_LIBRARY_PATH=/usr/ndp/current/mapreduce_client/lib/native:/usr/ndp/current/mapreduce_client/lib/native/Linux-amd64-64:$LD_LIBRARY_PATH

```

**spark-default.conf配置如下**：

需要额外添加的配置项为`spark.history.store.path` `spark.history.store.maxDiskUsage`  ,spark.history.store.path 需要手动创建目录，spark.history.store.maxDiskUsage 设为20g。

其他所有配置是从老版本history server中拷贝，然后`spark.history.fs.cleaner.maxAge`调大至15d，`spark.history.ui.maxApplications`调大至50000(后续如果spark每天的应用数更多，可以调至100000),另外`spark.history.fs.numReplayThreads`设置为6(重要，因为将`spark.history.ui.maxApplications`调大之后，第一次启动时会拉取大量的数据，目前集群上设置的线程数是20，线程过多会造成网卡超载，目前线上机器核数为24，建议设置为核数的25%，即6)。

参考如下：

```
spark.driver.extraLibraryPath /usr/ndp/current/mapreduce_client/lib/native:/usr/ndp/current/mapreduce_client/lib/native/Linux-amd64-64
# 配置日志目录
spark.eventLog.dir hdfs://hz-cluster3/user/spark/history
spark.eventLog.enabled true
spark.executor.extraLibraryPath /usr/ndp/current/mapreduce_client/lib/native:/usr/ndp/current/mapreduce_client/lib/native/Linux-amd64-64
spark.history.fs.cleaner.interval 30min
# 设置为保存15天日志
spark.history.fs.cleaner.maxAge 15d
# 按集群情况更改
spark.history.fs.logDirectory hdfs://hz-cluster3/user/spark/history
# 推荐设置为机器核数的25%，若太高，会造成第一次启动时，网络流量过大
spark.history.fs.numReplayThreads 6
spark.history.fs.update.interval 60s
spark.history.kerberos.enabled true
# 需更改
spark.history.kerberos.principal                      hadoop/admin@HADOOP.HZ.NETEASE.COM
# 需更改
spark.history.kerberos.keytab                         /home/hadoop/yarn/conf/hadoop.keytab
spark.history.provider org.apache.spark.deploy.history.FsHistoryProvider
spark.history.retainedApplications 50
spark.history.ui.maxApplications 50000
spark.history.ui.port 18080
spark.yarn.historyServer.address spark1.lt.163.org:18080
spark.yarn.queue default
# 必须设置
spark.history.store.path     /home/hadoop/spark-2.3.2-bin-his-0.1/historyStore
spark.history.store.maxDiskUsage   20g
```

然后关掉现有的spark history server，然后启动该新版history server。

在线上将spark.history.ui.maxApplications 设置为50000，可以显示50000个application，可以看到线上近一个月左右的应用。

## 4、其他说明

**spark.history.store.path**

**spark.history.store.path**， 是单盘设置，localPath（会在文档中补充创建在history server所在节点）。这个路径默认是空，如果不设置会把数据放置在内存。目前在spark1.lt上的运行情况，spark.history.ui.maxApplications设置为50000,目前可以看到集群上近一个月的数据，运行了3天，目前占用磁盘空间为176m，且这部分空间会定期进行清理，因此目前认为20g可以满足线上需求。



**时间戳显示**

关于spark history server的时区显示问题，如果发现history server页面时区为空。可以清除相关缓存js文件。把浏览器的相关缓存清除之后就显示正常了。