## Spark SQL CBO TPCDS测试

### 测试环境

测试使用工具为 tpcds, https://github.com/yaooqinn/tpcds-for-spark

六个节点，每个节点16g内存，四核，2.4GHZ

测试使用数据为100G.

Spark版本为官方包2.3.1,下载地址：http://mirrors.hust.edu.cn/apache/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz

集群存储空间使用如下，HDFS的replication设置为1.

![image-20181009191319414](/Users/bbw/todo/LearingAndThought/imgs/spark-cbo/hdfs-usage.png)

### 参数配置

通用参数配置如下：

```java
spark.master    yarn
spark.submit.deployMode   client
spark.serializer                                      org.apache.spark.serializer.KryoSerializer
spark.kryoserializer.buffer.max                       256m
# sql setting
spark.sql.shuffle.partitions   1024
spark.sql.crossJoin.enabled   true
spark.sql.autoBroadcastJoinThreshold          204857600
# executor setting
spark.executor.cores         2
spark.executor.memory  8g
spark.executor.instances      6
spark.executor.memoryOverhead     2048
spark.executor.extraJavaOptions  -XX:PermSize=1024m -XX:MaxPermSize=1024m -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution
## Dynamic Allocation Settings ##
spark.shuffle.service.enabled                         true
spark.dynamicAllocation.enabled                       false
spark.dynamicAllocation.initialExecutors              1
spark.dynamicAllocation.minExecutors                  1
spark.dynamicAllocation.maxExecutors                  20
## Driver/AM Settings ##
spark.yarn.am.cores                                   2
spark.yarn.am.memory                                  2g
spark.yarn.am.memoryOverhead                          512
spark.yarn.am.extraJavaOptions                        -XX:PermSize=1024m -XX:MaxPermSize=2048m -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution
spark.driver.maxResultSize                            2g
spark.driver.memory                                   8g
spark.driver.extraJavaOptions                         -XX:PermSize=1024m -XX:MaxPermSize=1024m
## Hadoop Settings ##
spark.hadoop.fs.hdfs.impl.disable.cache               true
spark.hadoop.fs.file.impl.disable.cache               true
spark.driver.maxResultSize     4g
```

测试分为两组，一组是开了CBO的，一组是对照组没开启CBO参数。

CBO参数设置如下:

```
spark.sql.cbo.enabled=true 
spark.sql.cbo.joinReorder.dp.star.filter=true spark.sql.cbo.joinReorder.dp.threshold=12
spark.sql.cbo.joinReorder.enabled=true
spark.sql.cbo.starSchemaDetection=true

```

### CBO相关操作

tpcds 生成的数据在创建表之后，生成48张表。

CBO依赖一些统计数据来用于评估优化，因此需要使用命令来获得统计信息。如果不统计，会影响CBO的效果，因为我之前没有使用analyze table命令，在对比实验组和对照组的explain时发现无变化。

统计信息分为两种，一种是基本的表信息，另外一种是表中的列信息。

```SQL
# 统计表的条数以及大小信息
ANALYZE TABLE table_name COMPUTE STATISTICS
# 统计表中列数据的详细信息，最大值，最小值，平均长度，最大长度，为空数量等
ANALYZE TABLE table_name COMPUTE STATISTICS FOR COLUMNS column-name1, column-name2, ….
```

本次实验，对于CBO对照组，使用analyze table命令得到48张表所有列的统计信息。

共花费时间59min。



### 实验结果

![image-20181013153506127](../imgs/spark-cbo/tpcds-result.png)

这些sql，不使用CBO花费42883s，使用CBO花费23241s，加上analyze table使用的3600s，大概27000s，整体提升37%。

### 结果分析

看下query11.这是优化之前的query11的执行计划。

```mysql
== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[customer_id#276 ASC NULLS FIRST,customer_first_name#277 ASC NULLS FIRST,customer_last_name#278 ASC NULLS FIRST,customer_email_address#282 ASC NULLS FIRST], output=[customer_id#276,customer_first_name#277,customer_last_name#278,customer_email_address#282])
+- *(47) Project [customer_id#276, customer_first_name#277, customer_last_name#278, customer_email_address#282]
   +- *(47) SortMergeJoin [customer_id#0], [customer_id#296], Inner, (CASE WHEN (year_total#294 > 0.00) THEN CheckOverflow((promote_precision(year_total#304) / promote_precision(year_total#294)), DecimalType(38,20)) ELSE 0E-20 END > CASE WHEN (year_total#8 > 0.00) THEN CheckOverflow((promote_precision(year_total#284) / promote_precision(year_total#8)), DecimalType(38,20)) ELSE 0E-20 END)
      :- *(35) Project [customer_id#0, year_total#8, customer_id#276, customer_first_name#277, customer_last_name#278, customer_email_address#282, year_total#284, year_total#294]
      :  +- *(35) SortMergeJoin [customer_id#0], [customer_id#286], Inner
      :     :- *(23) SortMergeJoin [customer_id#0], [customer_id#276], Inner
      :     :  :- *(11) Sort [customer_id#0 ASC NULLS FIRST], false, 0
      :     :  :  +- Exchange hashpartitioning(customer_id#0, 1024)
      :     :  :     +- Union
      :     :  :        :- *(10) Filter (isnotnull(year_total#8) && (year_total#8 > 0.00))
      :     :  :        :  +- *(10) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69L], functions=[sum(UnscaledValue(CheckOverflow((promote_precision(cast(ss_ext_list_price#56 as decimal(8,2))) - promote_precision(cast(ss_ext_discount_amt#53 as decimal(8,2)))), DecimalType(8,2))))])
      :     :  :        :     +- Exchange hashpartitioning(c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69L, 1024)
      :     :  :        :        +- *(9) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69L], functions=[partial_sum(UnscaledValue(CheckOverflow((promote_precision(cast(ss_ext_list_price#56 as decimal(8,2))) - promote_precision(cast(ss_ext_discount_amt#53 as decimal(8,2)))), DecimalType(8,2))))])
      :     :  :        :           +- *(9) Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ss_ext_discount_amt#53, ss_ext_list_price#56, d_year#69L]
      :     :  :        :              +- *(9) SortMergeJoin [ss_sold_date_sk#62L], [d_date_sk#63L], Inner
      :     :  :        :                 :- *(6) Sort [ss_sold_date_sk#62L ASC NULLS FIRST], false, 0
      :     :  :        :                 :  +- Exchange hashpartitioning(ss_sold_date_sk#62L, 1024)
      :     :  :        :                 :     +- *(5) Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ss_ext_discount_amt#53, ss_ext_list_price#56, ss_sold_date_sk#62L]
      :     :  :        :                 :        +- *(5) SortMergeJoin [c_customer_sk#22L], [ss_customer_sk#42L], Inner
      :     :  :        :                 :           :- *(2) Sort [c_customer_sk#22L ASC NULLS FIRST], false, 0
      :     :  :        :                 :           :  +- Exchange hashpartitioning(c_customer_sk#22L, 1024)
      :     :  :        :                 :           :     +- *(1) Project [c_customer_sk#22L, c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38]
      :     :  :        :                 :           :        +- *(1) Filter (isnotnull(c_customer_sk#22L) && isnotnull(c_customer_id#23))
      :     :  :        :                 :           :           +- *(1) FileScan parquet tpcds_wangfei_100g.customer[c_customer_sk#22L,c_customer_id#23,c_first_name#30,c_last_name#31,c_preferred_cust_flag#32,c_birth_country#36,c_login#37,c_email_address#38] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/customer], PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_customer_id)], ReadSchema: struct<c_customer_sk:bigint,c_customer_id:string,c_first_name:string,c_last_name:string,c_preferr...
      :     :  :        :                 :           +- *(4) Sort [ss_customer_sk#42L ASC NULLS FIRST], false, 0
      :     :  :        :                 :              +- Exchange hashpartitioning(ss_customer_sk#42L, 1024)
      :     :  :        :                 :                 +- *(3) Project [ss_customer_sk#42L, ss_ext_discount_amt#53, ss_ext_list_price#56, ss_sold_date_sk#62L]
      :     :  :        :                 :                    +- *(3) Filter isnotnull(ss_customer_sk#42L)
      :     :  :        :                 :                       +- *(3) FileScan parquet tpcds_wangfei_100g.store_sales[ss_customer_sk#42L,ss_ext_discount_amt#53,ss_ext_list_price#56,ss_sold_date_sk#62L] Batched: true, Format: Parquet, Location: PrunedInMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/store_sales/ss_sold_date_..., PartitionCount: 1823, PartitionFilters: [isnotnull(ss_sold_date_sk#62L)], PushedFilters: [IsNotNull(ss_customer_sk)], ReadSchema: struct<ss_customer_sk:bigint,ss_ext_discount_amt:decimal(7,2),ss_ext_list_price:decimal(7,2)>
      :     :  :        :                 +- *(8) Sort [d_date_sk#63L ASC NULLS FIRST], false, 0
      :     :  :        :                    +- Exchange hashpartitioning(d_date_sk#63L, 1024)
      :     :  :        :                       +- *(7) Project [d_date_sk#63L, d_year#69L]
      :     :  :        :                          +- *(7) Filter ((isnotnull(d_year#69L) && (d_year#69L = 2001)) && isnotnull(d_date_sk#63L))
      :     :  :        :                             +- *(7) FileScan parquet tpcds_wangfei_100g.date_dim[d_date_sk#63L,d_year#69L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/date_dim], PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:bigint,d_year:bigint>
      :     :  :        +- LocalTableScan <empty>, [customer_id#10, year_total#18]
      :     :  +- *(22) Sort [customer_id#276 ASC NULLS FIRST], false, 0
      :     :     +- Exchange hashpartitioning(customer_id#276, 1024)
      :     :        +- Union
      :     :           :- *(21) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69L], functions=[sum(UnscaledValue(CheckOverflow((promote_precision(cast(ss_ext_list_price#56 as decimal(8,2))) - promote_precision(cast(ss_ext_discount_amt#53 as decimal(8,2)))), DecimalType(8,2))))])
      :     :           :  +- Exchange hashpartitioning(c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69L, 1024)
      :     :           :     +- *(20) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69L], functions=[partial_sum(UnscaledValue(CheckOverflow((promote_precision(cast(ss_ext_list_price#56 as decimal(8,2))) - promote_precision(cast(ss_ext_discount_amt#53 as decimal(8,2)))), DecimalType(8,2))))])
      :     :           :        +- *(20) Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ss_ext_discount_amt#53, ss_ext_list_price#56, d_year#69L]
      :     :           :           +- *(20) SortMergeJoin [ss_sold_date_sk#62L], [d_date_sk#63L], Inner
      :     :           :              :- *(17) Sort [ss_sold_date_sk#62L ASC NULLS FIRST], false, 0
      :     :           :              :  +- ReusedExchange [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ss_ext_discount_amt#53, ss_ext_list_price#56, ss_sold_date_sk#62L], Exchange hashpartitioning(ss_sold_date_sk#62L, 1024)
      :     :           :              +- *(19) Sort [d_date_sk#63L ASC NULLS FIRST], false, 0
      :     :           :                 +- Exchange hashpartitioning(d_date_sk#63L, 1024)
      :     :           :                    +- *(18) Project [d_date_sk#63L, d_year#69L]
      :     :           :                       +- *(18) Filter ((isnotnull(d_year#69L) && (d_year#69L = 2002)) && isnotnull(d_date_sk#63L))
      :     :           :                          +- *(18) FileScan parquet tpcds_wangfei_100g.date_dim[d_date_sk#63L,d_year#69L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/date_dim], PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2002), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:bigint,d_year:bigint>
      :     :           +- LocalTableScan <empty>, [customer_id#10, customer_first_name#11, customer_last_name#12, customer_email_address#16, year_total#18]
      :     +- *(34) Sort [customer_id#286 ASC NULLS FIRST], false, 0
      :        +- Exchange hashpartitioning(customer_id#286, 1024)
      :           +- Union
      :              :- LocalTableScan <empty>, [customer_id#286, year_total#294]
      :              +- *(33) Filter (isnotnull(year_total#18) && (year_total#18 > 0.00))
      :                 +- *(33) HashAggregate(keys=[c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, d_year#149L], functions=[sum(UnscaledValue(CheckOverflow((promote_precision(cast(ws_ext_list_price#133 as decimal(8,2))) - promote_precision(cast(ws_ext_discount_amt#130 as decimal(8,2)))), DecimalType(8,2))))])
      :                    +- Exchange hashpartitioning(c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, d_year#149L, 1024)
      :                       +- *(32) HashAggregate(keys=[c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, d_year#149L], functions=[partial_sum(UnscaledValue(CheckOverflow((promote_precision(cast(ws_ext_list_price#133 as decimal(8,2))) - promote_precision(cast(ws_ext_discount_amt#130 as decimal(8,2)))), DecimalType(8,2))))])
      :                          +- *(32) Project [c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, ws_ext_discount_amt#130, ws_ext_list_price#133, d_year#149L]
      :                             +- *(32) SortMergeJoin [ws_sold_date_sk#142L], [d_date_sk#143L], Inner
      :                                :- *(29) Sort [ws_sold_date_sk#142L ASC NULLS FIRST], false, 0
      :                                :  +- Exchange hashpartitioning(ws_sold_date_sk#142L, 1024)
      :                                :     +- *(28) Project [c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, ws_ext_discount_amt#130, ws_ext_list_price#133, ws_sold_date_sk#142L]
      :                                :        +- *(28) SortMergeJoin [c_customer_sk#91L], [ws_bill_customer_sk#112L], Inner
      :                                :           :- *(25) Sort [c_customer_sk#91L ASC NULLS FIRST], false, 0
      :                                :           :  +- ReusedExchange [c_customer_sk#91L, c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107], Exchange hashpartitioning(c_customer_sk#22L, 1024)
      :                                :           +- *(27) Sort [ws_bill_customer_sk#112L ASC NULLS FIRST], false, 0
      :                                :              +- Exchange hashpartitioning(ws_bill_customer_sk#112L, 1024)
      :                                :                 +- *(26) Project [ws_bill_customer_sk#112L, ws_ext_discount_amt#130, ws_ext_list_price#133, ws_sold_date_sk#142L]
      :                                :                    +- *(26) Filter isnotnull(ws_bill_customer_sk#112L)
      :                                :                       +- *(26) FileScan parquet tpcds_wangfei_100g.web_sales[ws_bill_customer_sk#112L,ws_ext_discount_amt#130,ws_ext_list_price#133,ws_sold_date_sk#142L] Batched: true, Format: Parquet, Location: PrunedInMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/web_sales/ws_sold_date_sk..., PartitionCount: 1823, PartitionFilters: [isnotnull(ws_sold_date_sk#142L)], PushedFilters: [IsNotNull(ws_bill_customer_sk)], ReadSchema: struct<ws_bill_customer_sk:bigint,ws_ext_discount_amt:decimal(7,2),ws_ext_list_price:decimal(7,2)>
      :                                +- *(31) Sort [d_date_sk#143L ASC NULLS FIRST], false, 0
      :                                   +- ReusedExchange [d_date_sk#143L, d_year#149L], Exchange hashpartitioning(d_date_sk#63L, 1024)
      +- *(46) Sort [customer_id#296 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(customer_id#296, 1024)
            +- Union
               :- LocalTableScan <empty>, [customer_id#296, year_total#304]
               +- *(45) HashAggregate(keys=[c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, d_year#149L], functions=[sum(UnscaledValue(CheckOverflow((promote_precision(cast(ws_ext_list_price#133 as decimal(8,2))) - promote_precision(cast(ws_ext_discount_amt#130 as decimal(8,2)))), DecimalType(8,2))))])
                  +- Exchange hashpartitioning(c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, d_year#149L, 1024)
                     +- *(44) HashAggregate(keys=[c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, d_year#149L], functions=[partial_sum(UnscaledValue(CheckOverflow((promote_precision(cast(ws_ext_list_price#133 as decimal(8,2))) - promote_precision(cast(ws_ext_discount_amt#130 as decimal(8,2)))), DecimalType(8,2))))])
                        +- *(44) Project [c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, ws_ext_discount_amt#130, ws_ext_list_price#133, d_year#149L]
                           +- *(44) SortMergeJoin [ws_sold_date_sk#142L], [d_date_sk#143L], Inner
                              :- *(41) Sort [ws_sold_date_sk#142L ASC NULLS FIRST], false, 0
                              :  +- ReusedExchange [c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, ws_ext_discount_amt#130, ws_ext_list_price#133, ws_sold_date_sk#142L], Exchange hashpartitioning(ws_sold_date_sk#142L, 1024)
                              +- *(43) Sort [d_date_sk#143L ASC NULLS FIRST], false, 0
                                 +- ReusedExchange [d_date_sk#143L, d_year#149L], Exchange hashpartitioning(d_date_sk#63L, 1024)
Time taken: 39.636 seconds, Fetched 1 row(s)
```

这是使用CBO之后的物理计划。

```mysql
== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[customer_id#276 ASC NULLS FIRST,customer_first_name#277 ASC NULLS FIRST,customer_last_name#278 ASC NULLS FIRST,customer_email_address#282 ASC NULLS FIRST], output=[customer_id#276,customer_first_name#277,customer_last_name#278,customer_email_address#282])
+- *(35) Project [customer_id#276, customer_first_name#277, customer_last_name#278, customer_email_address#282]
   +- *(35) SortMergeJoin [customer_id#0], [customer_id#296], Inner, (CASE WHEN (year_total#294 > 0.00) THEN CheckOverflow((promote_precision(year_total#304) / promote_precision(year_total#294)), DecimalType(38,20)) ELSE 0E-20 END > CASE WHEN (year_total#8 > 0.00) THEN CheckOverflow((promote_precision(year_total#284) / promote_precision(year_total#8)), DecimalType(38,20)) ELSE 0E-20 END)
      :- *(26) Project [customer_id#0, year_total#8, customer_id#276, customer_first_name#277, customer_last_name#278, customer_email_address#282, year_total#284, year_total#294]
      :  +- *(26) SortMergeJoin [customer_id#0], [customer_id#286], Inner
      :     :- *(17) SortMergeJoin [customer_id#0], [customer_id#276], Inner
      :     :  :- *(8) Sort [customer_id#0 ASC NULLS FIRST], false, 0
      :     :  :  +- Exchange hashpartitioning(customer_id#0, 1024)
      :     :  :     +- Union
      :     :  :        :- *(7) Filter (isnotnull(year_total#8) && (year_total#8 > 0.00))
      :     :  :        :  +- *(7) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69L], functions=[sum(UnscaledValue(CheckOverflow((promote_precision(cast(ss_ext_list_price#56 as decimal(8,2))) - promote_precision(cast(ss_ext_discount_amt#53 as decimal(8,2)))), DecimalType(8,2))))])
      :     :  :        :     +- Exchange hashpartitioning(c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69L, 1024)
      :     :  :        :        +- *(6) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69L], functions=[partial_sum(UnscaledValue(CheckOverflow((promote_precision(cast(ss_ext_list_price#56 as decimal(8,2))) - promote_precision(cast(ss_ext_discount_amt#53 as decimal(8,2)))), DecimalType(8,2))))])
      :     :  :        :           +- *(6) Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ss_ext_discount_amt#53, ss_ext_list_price#56, d_year#69L]
      :     :  :        :              +- *(6) SortMergeJoin [ss_customer_sk#42L], [c_customer_sk#22L], Inner
      :     :  :        :                 :- *(3) Sort [ss_customer_sk#42L ASC NULLS FIRST], false, 0
      :     :  :        :                 :  +- Exchange hashpartitioning(ss_customer_sk#42L, 1024)
      :     :  :        :                 :     +- *(2) Project [ss_customer_sk#42L, ss_ext_discount_amt#53, ss_ext_list_price#56, d_year#69L]
      :     :  :        :                 :        +- *(2) BroadcastHashJoin [ss_sold_date_sk#62L], [d_date_sk#63L], Inner, BuildRight
      :     :  :        :                 :           :- *(2) Project [ss_customer_sk#42L, ss_ext_discount_amt#53, ss_ext_list_price#56, ss_sold_date_sk#62L]
      :     :  :        :                 :           :  +- *(2) Filter isnotnull(ss_customer_sk#42L)
      :     :  :        :                 :           :     +- *(2) FileScan parquet tpcds_wangfei_100g.store_sales[ss_customer_sk#42L,ss_ext_discount_amt#53,ss_ext_list_price#56,ss_sold_date_sk#62L] Batched: true, Format: Parquet, Location: PrunedInMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/store_sales/ss_sold_date_..., PartitionCount: 1823, PartitionFilters: [isnotnull(ss_sold_date_sk#62L)], PushedFilters: [IsNotNull(ss_customer_sk)], ReadSchema: struct<ss_customer_sk:bigint,ss_ext_discount_amt:decimal(7,2),ss_ext_list_price:decimal(7,2)>
      :     :  :        :                 :           +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
      :     :  :        :                 :              +- *(1) Project [d_date_sk#63L, d_year#69L]
      :     :  :        :                 :                 +- *(1) Filter ((isnotnull(d_year#69L) && (d_year#69L = 2001)) && isnotnull(d_date_sk#63L))
      :     :  :        :                 :                    +- *(1) FileScan parquet tpcds_wangfei_100g.date_dim[d_date_sk#63L,d_year#69L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/date_dim], PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2001), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:bigint,d_year:bigint>
      :     :  :        :                 +- *(5) Sort [c_customer_sk#22L ASC NULLS FIRST], false, 0
      :     :  :        :                    +- Exchange hashpartitioning(c_customer_sk#22L, 1024)
      :     :  :        :                       +- *(4) Project [c_customer_sk#22L, c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38]
      :     :  :        :                          +- *(4) Filter (isnotnull(c_customer_sk#22L) && isnotnull(c_customer_id#23))
      :     :  :        :                             +- *(4) FileScan parquet tpcds_wangfei_100g.customer[c_customer_sk#22L,c_customer_id#23,c_first_name#30,c_last_name#31,c_preferred_cust_flag#32,c_birth_country#36,c_login#37,c_email_address#38] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/customer], PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk), IsNotNull(c_customer_id)], ReadSchema: struct<c_customer_sk:bigint,c_customer_id:string,c_first_name:string,c_last_name:string,c_preferr...
      :     :  :        +- LocalTableScan <empty>, [customer_id#10, year_total#18]
      :     :  +- *(16) Sort [customer_id#276 ASC NULLS FIRST], false, 0
      :     :     +- Exchange hashpartitioning(customer_id#276, 1024)
      :     :        +- Union
      :     :           :- *(15) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69L], functions=[sum(UnscaledValue(CheckOverflow((promote_precision(cast(ss_ext_list_price#56 as decimal(8,2))) - promote_precision(cast(ss_ext_discount_amt#53 as decimal(8,2)))), DecimalType(8,2))))])
      :     :           :  +- Exchange hashpartitioning(c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69L, 1024)
      :     :           :     +- *(14) HashAggregate(keys=[c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, d_year#69L], functions=[partial_sum(UnscaledValue(CheckOverflow((promote_precision(cast(ss_ext_list_price#56 as decimal(8,2))) - promote_precision(cast(ss_ext_discount_amt#53 as decimal(8,2)))), DecimalType(8,2))))])
      :     :           :        +- *(14) Project [c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38, ss_ext_discount_amt#53, ss_ext_list_price#56, d_year#69L]
      :     :           :           +- *(14) SortMergeJoin [ss_customer_sk#42L], [c_customer_sk#22L], Inner
      :     :           :              :- *(11) Sort [ss_customer_sk#42L ASC NULLS FIRST], false, 0
      :     :           :              :  +- Exchange hashpartitioning(ss_customer_sk#42L, 1024)
      :     :           :              :     +- *(10) Project [ss_customer_sk#42L, ss_ext_discount_amt#53, ss_ext_list_price#56, d_year#69L]
      :     :           :              :        +- *(10) BroadcastHashJoin [ss_sold_date_sk#62L], [d_date_sk#63L], Inner, BuildRight
      :     :           :              :           :- *(10) Project [ss_customer_sk#42L, ss_ext_discount_amt#53, ss_ext_list_price#56, ss_sold_date_sk#62L]
      :     :           :              :           :  +- *(10) Filter isnotnull(ss_customer_sk#42L)
      :     :           :              :           :     +- *(10) FileScan parquet tpcds_wangfei_100g.store_sales[ss_customer_sk#42L,ss_ext_discount_amt#53,ss_ext_list_price#56,ss_sold_date_sk#62L] Batched: true, Format: Parquet, Location: PrunedInMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/store_sales/ss_sold_date_..., PartitionCount: 1823, PartitionFilters: [isnotnull(ss_sold_date_sk#62L)], PushedFilters: [IsNotNull(ss_customer_sk)], ReadSchema: struct<ss_customer_sk:bigint,ss_ext_discount_amt:decimal(7,2),ss_ext_list_price:decimal(7,2)>
      :     :           :              :           +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
      :     :           :              :              +- *(9) Project [d_date_sk#63L, d_year#69L]
      :     :           :              :                 +- *(9) Filter ((isnotnull(d_year#69L) && (d_year#69L = 2002)) && isnotnull(d_date_sk#63L))
      :     :           :              :                    +- *(9) FileScan parquet tpcds_wangfei_100g.date_dim[d_date_sk#63L,d_year#69L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/date_dim], PartitionFilters: [], PushedFilters: [IsNotNull(d_year), EqualTo(d_year,2002), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:bigint,d_year:bigint>
      :     :           :              +- *(13) Sort [c_customer_sk#22L ASC NULLS FIRST], false, 0
      :     :           :                 +- ReusedExchange [c_customer_sk#22L, c_customer_id#23, c_first_name#30, c_last_name#31, c_preferred_cust_flag#32, c_birth_country#36, c_login#37, c_email_address#38], Exchange hashpartitioning(c_customer_sk#22L, 1024)
      :     :           +- LocalTableScan <empty>, [customer_id#10, customer_first_name#11, customer_last_name#12, customer_email_address#16, year_total#18]
      :     +- *(25) Sort [customer_id#286 ASC NULLS FIRST], false, 0
      :        +- Exchange hashpartitioning(customer_id#286, 1024)
      :           +- Union
      :              :- LocalTableScan <empty>, [customer_id#286, year_total#294]
      :              +- *(24) Filter (isnotnull(year_total#18) && (year_total#18 > 0.00))
      :                 +- *(24) HashAggregate(keys=[c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, d_year#149L], functions=[sum(UnscaledValue(CheckOverflow((promote_precision(cast(ws_ext_list_price#133 as decimal(8,2))) - promote_precision(cast(ws_ext_discount_amt#130 as decimal(8,2)))), DecimalType(8,2))))])
      :                    +- Exchange hashpartitioning(c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, d_year#149L, 1024)
      :                       +- *(23) HashAggregate(keys=[c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, d_year#149L], functions=[partial_sum(UnscaledValue(CheckOverflow((promote_precision(cast(ws_ext_list_price#133 as decimal(8,2))) - promote_precision(cast(ws_ext_discount_amt#130 as decimal(8,2)))), DecimalType(8,2))))])
      :                          +- *(23) Project [c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, ws_ext_discount_amt#130, ws_ext_list_price#133, d_year#149L]
      :                             +- *(23) SortMergeJoin [ws_bill_customer_sk#112L], [c_customer_sk#91L], Inner
      :                                :- *(20) Sort [ws_bill_customer_sk#112L ASC NULLS FIRST], false, 0
      :                                :  +- Exchange hashpartitioning(ws_bill_customer_sk#112L, 1024)
      :                                :     +- *(19) Project [ws_bill_customer_sk#112L, ws_ext_discount_amt#130, ws_ext_list_price#133, d_year#149L]
      :                                :        +- *(19) BroadcastHashJoin [ws_sold_date_sk#142L], [d_date_sk#143L], Inner, BuildRight
      :                                :           :- *(19) Project [ws_bill_customer_sk#112L, ws_ext_discount_amt#130, ws_ext_list_price#133, ws_sold_date_sk#142L]
      :                                :           :  +- *(19) Filter isnotnull(ws_bill_customer_sk#112L)
      :                                :           :     +- *(19) FileScan parquet tpcds_wangfei_100g.web_sales[ws_bill_customer_sk#112L,ws_ext_discount_amt#130,ws_ext_list_price#133,ws_sold_date_sk#142L] Batched: true, Format: Parquet, Location: PrunedInMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/web_sales/ws_sold_date_sk..., PartitionCount: 1823, PartitionFilters: [isnotnull(ws_sold_date_sk#142L)], PushedFilters: [IsNotNull(ws_bill_customer_sk)], ReadSchema: struct<ws_bill_customer_sk:bigint,ws_ext_discount_amt:decimal(7,2),ws_ext_list_price:decimal(7,2)>
      :                                :           +- ReusedExchange [d_date_sk#143L, d_year#149L], BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
      :                                +- *(22) Sort [c_customer_sk#91L ASC NULLS FIRST], false, 0
      :                                   +- ReusedExchange [c_customer_sk#91L, c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107], Exchange hashpartitioning(c_customer_sk#22L, 1024)
      +- *(34) Sort [customer_id#296 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(customer_id#296, 1024)
            +- Union
               :- LocalTableScan <empty>, [customer_id#296, year_total#304]
               +- *(33) HashAggregate(keys=[c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, d_year#149L], functions=[sum(UnscaledValue(CheckOverflow((promote_precision(cast(ws_ext_list_price#133 as decimal(8,2))) - promote_precision(cast(ws_ext_discount_amt#130 as decimal(8,2)))), DecimalType(8,2))))])
                  +- Exchange hashpartitioning(c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, d_year#149L, 1024)
                     +- *(32) HashAggregate(keys=[c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, d_year#149L], functions=[partial_sum(UnscaledValue(CheckOverflow((promote_precision(cast(ws_ext_list_price#133 as decimal(8,2))) - promote_precision(cast(ws_ext_discount_amt#130 as decimal(8,2)))), DecimalType(8,2))))])
                        +- *(32) Project [c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107, ws_ext_discount_amt#130, ws_ext_list_price#133, d_year#149L]
                           +- *(32) SortMergeJoin [ws_bill_customer_sk#112L], [c_customer_sk#91L], Inner
                              :- *(29) Sort [ws_bill_customer_sk#112L ASC NULLS FIRST], false, 0
                              :  +- Exchange hashpartitioning(ws_bill_customer_sk#112L, 1024)
                              :     +- *(28) Project [ws_bill_customer_sk#112L, ws_ext_discount_amt#130, ws_ext_list_price#133, d_year#149L]
                              :        +- *(28) BroadcastHashJoin [ws_sold_date_sk#142L], [d_date_sk#143L], Inner, BuildRight
                              :           :- *(28) Project [ws_bill_customer_sk#112L, ws_ext_discount_amt#130, ws_ext_list_price#133, ws_sold_date_sk#142L]
                              :           :  +- *(28) Filter isnotnull(ws_bill_customer_sk#112L)
                              :           :     +- *(28) FileScan parquet tpcds_wangfei_100g.web_sales[ws_bill_customer_sk#112L,ws_ext_discount_amt#130,ws_ext_list_price#133,ws_sold_date_sk#142L] Batched: true, Format: Parquet, Location: PrunedInMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/web_sales/ws_sold_date_sk..., PartitionCount: 1823, PartitionFilters: [isnotnull(ws_sold_date_sk#142L)], PushedFilters: [IsNotNull(ws_bill_customer_sk)], ReadSchema: struct<ws_bill_customer_sk:bigint,ws_ext_discount_amt:decimal(7,2),ws_ext_list_price:decimal(7,2)>
                              :           +- ReusedExchange [d_date_sk#143L, d_year#149L], BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
                              +- *(31) Sort [c_customer_sk#91L ASC NULLS FIRST], false, 0
                                 +- ReusedExchange [c_customer_sk#91L, c_customer_id#92, c_first_name#99, c_last_name#100, c_preferred_cust_flag#101, c_birth_country#105, c_login#106, c_email_address#107], Exchange hashpartitioning(c_customer_sk#22L, 1024)
Time taken: 35.406 seconds, Fetched 1 row(s)
```

首先比较明显的就是，在cbo之后，使用broadcast join代替了cbo之前的sortMergejoin，性能提升很大。

这就关乎cbo开启之前和之后的Statistics准确度问题。

在`spark cbo源码分析`里面讲过，如果没有开启CBO，如果join类型不是leftSemi或者leftAnti join，则将两表大小之乘积作为预估大小，且在整条plan tree的估计都是粗糙的，会放大误差，造成这里预估的值大于 阈值，从而采取了不合适的join方法。