<div id="EXPLAIN11"></div>

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





**query25**

优化之后:

```
== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[i_item_id#196 ASC NULLS FIRST,i_item_desc#199 ASC NULLS FIRST,s_store_id#167 ASC NULLS FIRST,s_store_name#171 ASC NULLS FIRST], output=[i_item_id#196,i_item_desc#199,s_store_id#167,s_store_name#171,store_sales_profit#0,store_returns_loss#1,catalog_sales_profit#2])
+- *(15) HashAggregate(keys=[i_item_id#196, i_item_desc#199, s_store_id#167, s_store_name#171], functions=[sum(UnscaledValue(ss_net_profit#26)), sum(UnscaledValue(sr_net_loss#46)), sum(UnscaledValue(cs_net_profit#80))])
   +- Exchange hashpartitioning(i_item_id#196, i_item_desc#199, s_store_id#167, s_store_name#171, 1024)
      +- *(14) HashAggregate(keys=[i_item_id#196, i_item_desc#199, s_store_id#167, s_store_name#171], functions=[partial_sum(UnscaledValue(ss_net_profit#26)), partial_sum(UnscaledValue(sr_net_loss#46)), partial_sum(UnscaledValue(cs_net_profit#80))])
         +- *(14) Project [ss_net_profit#26, sr_net_loss#46, cs_net_profit#80, s_store_id#167, s_store_name#171, i_item_id#196, i_item_desc#199]
            +- *(14) SortMergeJoin [sr_customer_sk#30L, sr_item_sk#29L], [cs_bill_customer_sk#50L, cs_item_sk#62L], Inner
               :- *(10) Sort [sr_customer_sk#30L ASC NULLS FIRST, sr_item_sk#29L ASC NULLS FIRST], false, 0
               :  +- Exchange hashpartitioning(sr_customer_sk#30L, sr_item_sk#29L, 1024)
               :     +- *(9) Project [ss_net_profit#26, s_store_id#167, s_store_name#171, i_item_id#196, i_item_desc#199, sr_item_sk#29L, sr_customer_sk#30L, sr_net_loss#46]
               :        +- *(9) SortMergeJoin [ss_item_sk#6L, ss_customer_sk#7L, ss_ticket_number#13L], [sr_item_sk#29L, sr_customer_sk#30L, sr_ticket_number#36L], Inner
               :           :- *(5) Sort [ss_item_sk#6L ASC NULLS FIRST, ss_customer_sk#7L ASC NULLS FIRST, ss_ticket_number#13L ASC NULLS FIRST], false, 0
               :           :  +- Exchange hashpartitioning(ss_item_sk#6L, ss_customer_sk#7L, ss_ticket_number#13L, 1024)
               :           :     +- *(4) Project [ss_item_sk#6L, ss_customer_sk#7L, ss_ticket_number#13L, ss_net_profit#26, s_store_id#167, s_store_name#171, i_item_id#196, i_item_desc#199]
               :           :        +- *(4) BroadcastHashJoin [ss_item_sk#6L], [i_item_sk#195L], Inner, BuildRight
               :           :           :- *(4) Project [ss_item_sk#6L, ss_customer_sk#7L, ss_ticket_number#13L, ss_net_profit#26, s_store_id#167, s_store_name#171]
               :           :           :  +- *(4) BroadcastHashJoin [ss_store_sk#11L], [s_store_sk#166L], Inner, BuildRight
               :           :           :     :- *(4) Project [ss_item_sk#6L, ss_customer_sk#7L, ss_store_sk#11L, ss_ticket_number#13L, ss_net_profit#26]
               :           :           :     :  +- *(4) BroadcastHashJoin [ss_sold_date_sk#27L], [d_date_sk#82L], Inner, BuildRight
               :           :           :     :     :- *(4) Project [ss_item_sk#6L, ss_customer_sk#7L, ss_store_sk#11L, ss_ticket_number#13L, ss_net_profit#26, ss_sold_date_sk#27L]
               :           :           :     :     :  +- *(4) Filter (((isnotnull(ss_ticket_number#13L) && isnotnull(ss_item_sk#6L)) && isnotnull(ss_customer_sk#7L)) && isnotnull(ss_store_sk#11L))
               :           :           :     :     :     +- *(4) FileScan parquet tpcds_wangfei_100g.store_sales[ss_item_sk#6L,ss_customer_sk#7L,ss_store_sk#11L,ss_ticket_number#13L,ss_net_profit#26,ss_sold_date_sk#27L] Batched: true, Format: Parquet, Location: PrunedInMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/store_sales/ss_sold_date_..., PartitionCount: 1823, PartitionFilters: [isnotnull(ss_sold_date_sk#27L)], PushedFilters: [IsNotNull(ss_ticket_number), IsNotNull(ss_item_sk), IsNotNull(ss_customer_sk), IsNotNull(ss_stor..., ReadSchema: struct<ss_item_sk:bigint,ss_customer_sk:bigint,ss_store_sk:bigint,ss_ticket_number:bigint,ss_net_...
               :           :           :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
               :           :           :     :        +- *(1) Project [d_date_sk#82L]
               :           :           :     :           +- *(1) Filter ((((isnotnull(d_moy#90L) && isnotnull(d_year#88L)) && (d_moy#90L = 4)) && (d_year#88L = 2000)) && isnotnull(d_date_sk#82L))
               :           :           :     :              +- *(1) FileScan parquet tpcds_wangfei_100g.date_dim[d_date_sk#82L,d_year#88L,d_moy#90L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/date_dim], PartitionFilters: [], PushedFilters: [IsNotNull(d_moy), IsNotNull(d_year), EqualTo(d_moy,4), EqualTo(d_year,2000), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:bigint,d_year:bigint,d_moy:bigint>
               :           :           :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
               :           :           :        +- *(2) Project [s_store_sk#166L, s_store_id#167, s_store_name#171]
               :           :           :           +- *(2) Filter isnotnull(s_store_sk#166L)
               :           :           :              +- *(2) FileScan parquet tpcds_wangfei_100g.store[s_store_sk#166L,s_store_id#167,s_store_name#171] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/store], PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:bigint,s_store_id:string,s_store_name:string>
               :           :           +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
               :           :              +- *(3) Project [i_item_sk#195L, i_item_id#196, i_item_desc#199]
               :           :                 +- *(3) Filter isnotnull(i_item_sk#195L)
               :           :                    +- *(3) FileScan parquet tpcds_wangfei_100g.item[i_item_sk#195L,i_item_id#196,i_item_desc#199] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/item], PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:bigint,i_item_id:string,i_item_desc:string>
               :           +- *(8) Sort [sr_item_sk#29L ASC NULLS FIRST, sr_customer_sk#30L ASC NULLS FIRST, sr_ticket_number#36L ASC NULLS FIRST], false, 0
               :              +- Exchange hashpartitioning(sr_item_sk#29L, sr_customer_sk#30L, sr_ticket_number#36L, 1024)
               :                 +- *(7) Project [sr_item_sk#29L, sr_customer_sk#30L, sr_ticket_number#36L, sr_net_loss#46]
               :                    +- *(7) BroadcastHashJoin [d_date_sk#110L], [sr_returned_date_sk#47L], Inner, BuildLeft
               :                       :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
               :                       :  +- *(6) Project [d_date_sk#110L]
               :                       :     +- *(6) Filter (((((isnotnull(d_year#116L) && isnotnull(d_moy#118L)) && (d_moy#118L >= 4)) && (d_moy#118L <= 10)) && (d_year#116L = 2000)) && isnotnull(d_date_sk#110L))
               :                       :        +- *(6) FileScan parquet tpcds_wangfei_100g.date_dim[d_date_sk#110L,d_year#116L,d_moy#118L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/date_dim], PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), GreaterThanOrEqual(d_moy,4), LessThanOrEqual(d_moy,10), Equ..., ReadSchema: struct<d_date_sk:bigint,d_year:bigint,d_moy:bigint>
               :                       +- *(7) Project [sr_item_sk#29L, sr_customer_sk#30L, sr_ticket_number#36L, sr_net_loss#46, sr_returned_date_sk#47L]
               :                          +- *(7) Filter ((isnotnull(sr_ticket_number#36L) && isnotnull(sr_customer_sk#30L)) && isnotnull(sr_item_sk#29L))
               :                             +- *(7) FileScan parquet tpcds_wangfei_100g.store_returns[sr_item_sk#29L,sr_customer_sk#30L,sr_ticket_number#36L,sr_net_loss#46,sr_returned_date_sk#47L] Batched: true, Format: Parquet, Location: PrunedInMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/store_returns/sr_returned..., PartitionCount: 2003, PartitionFilters: [isnotnull(sr_returned_date_sk#47L)], PushedFilters: [IsNotNull(sr_ticket_number), IsNotNull(sr_customer_sk), IsNotNull(sr_item_sk)], ReadSchema: struct<sr_item_sk:bigint,sr_customer_sk:bigint,sr_ticket_number:bigint,sr_net_loss:decimal(7,2)>
               +- *(13) Sort [cs_bill_customer_sk#50L ASC NULLS FIRST, cs_item_sk#62L ASC NULLS FIRST], false, 0
                  +- Exchange hashpartitioning(cs_bill_customer_sk#50L, cs_item_sk#62L, 1024)
                     +- *(12) Project [cs_bill_customer_sk#50L, cs_item_sk#62L, cs_net_profit#80]
                        +- *(12) BroadcastHashJoin [cs_sold_date_sk#81L], [d_date_sk#138L], Inner, BuildRight
                           :- *(12) Project [cs_bill_customer_sk#50L, cs_item_sk#62L, cs_net_profit#80, cs_sold_date_sk#81L]
                           :  +- *(12) Filter (isnotnull(cs_item_sk#62L) && isnotnull(cs_bill_customer_sk#50L))
                           :     +- *(12) FileScan parquet tpcds_wangfei_100g.catalog_sales[cs_bill_customer_sk#50L,cs_item_sk#62L,cs_net_profit#80,cs_sold_date_sk#81L] Batched: true, Format: Parquet, Location: PrunedInMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/catalog_sales/cs_sold_dat..., PartitionCount: 1836, PartitionFilters: [isnotnull(cs_sold_date_sk#81L)], PushedFilters: [IsNotNull(cs_item_sk), IsNotNull(cs_bill_customer_sk)], ReadSchema: struct<cs_bill_customer_sk:bigint,cs_item_sk:bigint,cs_net_profit:decimal(7,2)>
                           +- ReusedExchange [d_date_sk#138L], BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
Time taken: 40.836 seconds, Fetched 1 row(s)
```