alluxio是一个VDFS。对接各种DFS 外加一个buffer。



统一存储管理



intelligent cache

etl

files jni 性能损失

本身保存元数据，under store

相当于一层缓存

单点 ，一致性，元数据一致性，异步写，双写

大数据文件append ，不支持修改（如何修改）

写本地性，可以写多份，然后删除冗余。



hdfs replication固定，alluxio 根据数据热度来自动调节

但是热数据会造成太多数据副本。参数设置上限

**zookeeper 的shared storage性能问题，使用raft实现leader选举**



**grpc <-netty thrift** 



**lock 并行化粒度**



**logging** 





携程案例

​                                  

Federation 拆库拆表  美团



merge小文件

监控临时表读写，删除长时间未读写表

\### spark streaming小文件

\### 跨集群 hdfs

\### RPC请求



alluxio，可以将多集群hdfs统一

alluxio提供ttl功能，及时清理streaming生成的小文件， 小文件放统一位置，容易监控

alluxio缓存部分数据，缓解namenode rpc请求。



presto？是啥

权限管理