# Spark History Server升级文档

## 包地址
安装包地址为：http://repo.bdms.netease.com/release_packages/common/spark/spark-2.3.2-bin-ne-0.0.0.tgz

md5文件： http://repo.bdms.netease.com/release_packages/common/spark/spark-2.3.2-bin-ne-0.0.0.tgz.md5

md5:  ae924a4ddba28319cc39e108271293e5  spark-2.3.2-bin-ne-0.0.0.tgz

## 社区基线

2.3.2

## 版本说明

> **该版本基于社区2.3.2**

> > NE-Spark - 版本号 ne-spark-2.3.2-0.0.0
> >
> > 解决线上问题
> >
> > > spark history server对于用户和平台开发人员来说都是一个很有效的工具，可以帮助很好的把握应用的运行状况和定位问题。
> > >
> > > 目前线上运行的spark history server是基于社区2.1.2，这些老版本的history server存在以下问题：
> > >
> > > 1、老版本的history server将一切数据都存储在内存里，不依赖外部存储，这样的history server没有状态，在需要进行重启时，一切数据都要重新加载，处理。
> > >
> > > 2、老版本的history server可以查看的application list 很有限。因为需要把一切数据都放在内存，所以可查看的application list数目受到限制，目前线上的上限是2000条左右，甚至当天的application都不能查看日志，远远不能满足生产需求。
> > >
> > > 3、老版本的history server需要把一切数据都缓存在内存，如果内存占用严重，发生full gc时可能会造成不能及时查看应用日志。
> >
> > 新版本的history server解决了这些问题，依赖外部kv存储，可以将history server的数据和状态进行存储，缓解内存压力，可以支持查看大量的application 日志，满足生产需求，且在需要重启时，可以读取外部kv存储的数据，做到快速启动。
> >
> > 测试情况：
> >
> > > 此前在线下测试了功能性和兼容性，测试结果表明功能性完好，兼容spark 各个版本的log(spark-2.1.2 spark-1.6.3).
> > >
> > > 目前已经在spark1.lt.163.org节点试运行半个月左右，设置查看application数量为50000，可以看到集群上近一个月application的日志, 运行稳定。

## 缺陷

[NESPARK-148](http://jira.netease.com/browse/NESPARK-148)-[NE]\[2.1.2]The problems in current online Spark History Server

## 任务

[NESPARK-141](http://jira.netease.com/browse/NESPARK-141)-\[NE\]\[2.3.2\]Applying History Server on our online environments

## 配置增改

| 配置项                           | 配置文件           | 默认值 | 配置值                           | 功能简介                                                     |
| -------------------------------- | ------------------ | ------ | -------------------------------- | ------------------------------------------------------------ |
| spark.history.store.path         | spark-default.conf | null   | /usr/ndp/data/spark/historyStore | 用于缓存history  数据的本地文件夹，默认为空。如果不设置，所有数据将会放在内存中。 |
| spark.history.store.maxDiskUsage | spark-default.conf | 10g    | 20g                              | spark.history.store.path可以使用的最大磁盘空间               |

## 限制说明

无

## 升级指导

### 手动升级

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

其他所有配置都可从老版本history server中拷贝，然后`spark.history.fs.cleaner.maxAge`调大至15d，`spark.history.ui.maxApplications`调大至50000(后续如果spark每天的应用数更多，可以调至100000),另外`spark.history.fs.numReplayThreads`设置为6(重要，因为将`spark.history.ui.maxApplications`调大之后，第一次启动时会拉取大量的数据，目前集群上设置的线程数是20，线程过多会造成网卡超载，目前线上机器核数为24，建议设置为核数的25%，即6)。

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

### Ambari 部署

#### 1、打开包更新开关：

```bash
# 根据实际环境修改相应字段信息
/var/lib/ambari-server/resources/scripts/configs.py --user admin --password admin --action set --host hzadg-ambari-dev1.server.163.org --cluster dev --config-type cluster-env --key reinstall_component_package --value true
      
```

##### 2、在ambari页面的 spark2配置页面的configs页面下的Advanced spark2-env模块更改SPARK2_JOBHISTORYSERVER对应的安装包位置。



![](/Users/bbw/todo/LearingAndThought/imgs/spark-his/image1.png)

![image-20181010162750358](/Users/bbw/todo/LearingAndThought/imgs/spark-his/package-path.png)

####  3、调整spark history server相关参数，如下：

增大spark_daemon_memory至40g

![image-20181010113823534](/Users/bbw/todo/LearingAndThought/imgs/spark-his/memory-tune.png)

在 Spark2-thrift-sparkconf中修改（或者搜索这些配置项看在哪个模块中配置)

![image-20181010114726561](/Users/bbw/todo/LearingAndThought/imgs/spark-his/his-cnf.png)

然后在 Custom spark2-defaults中添加配置项，如下。

![image-20181010163929862](/Users/bbw/todo/LearingAndThought/imgs/spark-his/other-cnf.png)

这里的`spark.history.store.path`需要注意，**这是一个local目录，这个文件夹需要创建在spark-history-server所在节点**。因为此处spark.history.kerberos.keytab使用的是`/etc/security/keytabs/spark2.headless.keytab`。在认证之后,

```
2018-10-10 16:37:16,195 [1222] - INFO  [main:Logging$class@54] - Changing view acls to: spark
2018-10-10 16:37:16,196 [1223] - INFO  [main:Logging$class@54] - Changing modify acls to: spark
```

读写权限同用户`spark`，因此在创建spark.history.store.path 之后，需要使用  ` chown -R spark /usr/ndp/data/spark/historyStore ` 赋权。



#### 4、在配置好之后通过ambari重启spark history server。

#### 5、 关闭包更新开关：

```bash
# 根据实际环境修改相应字段信息
/var/lib/ambari-server/resources/scripts/configs.py --user admin --password admin --action set --host hzadg-ambari-dev1.server.163.org --cluster dev --config-type cluster-env --key reinstall_component_package --value false
```





## 回滚方案



手动升级因为是独立的安装目录，只需要重新按照老版本安装包重新启动。

ambari部署，需要将配置的参数重置，然后重启。



## 依赖组件

无

## 对线上的主要影响及风险点

在第一次启动时，需要拉取大量的event log，因此需要耗时一到两小时，期间不能查看spark history 日志。

风险点：首次启动时，如果说某些大作业eventLog较多，因为historyServer是单机运行，会造成大量网络传输，首次启动需要通过哨兵观察网络使用情况。