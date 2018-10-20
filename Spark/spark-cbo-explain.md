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





**query50**

优化之前：

```scala
== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[s_store_name#55 ASC NULLS FIRST,s_company_id#66 ASC NULLS FIRST,s_street_number#68 ASC NULLS FIRST,s_street_name#69 ASC NULLS FIRST,s_street_type#70 ASC NULLS FIRST,s_suite_number#71 ASC NULLS FIRST,s_city#72 ASC NULLS FIRST,s_county#73 ASC NULLS FIRST,s_state#74 ASC NULLS FIRST,s_zip#75 ASC NULLS FIRST], output=[s_store_name#55,s_company_id#66,s_street_number#68,s_street_name#69,s_street_type#70,s_suite_number#71,s_city#72,s_county#73,s_state#74,s_zip#75,30days#0L,3160days#1L,6190days#2L,91120days#3L,dy120days#4L])
+- *(18) HashAggregate(keys=[s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75], functions=[sum(cast(CASE WHEN ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 30) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 30) && ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 60)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 60) && ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 90)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 90) && ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 120)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 120) THEN 1 ELSE 0 END as bigint))])
   +- Exchange hashpartitioning(s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75, 1024)
      +- *(17) HashAggregate(keys=[s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75], functions=[partial_sum(cast(CASE WHEN ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 30) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 30) && ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 60)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 60) && ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 90)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 90) && ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 120)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 120) THEN 1 ELSE 0 END as bigint))])
         +- *(17) Project [ss_sold_date_sk#29L, sr_returned_date_sk#49L, s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75]
            +- *(17) SortMergeJoin [sr_returned_date_sk#49L], [d_date_sk#107L], Inner
               :- *(14) Sort [sr_returned_date_sk#49L ASC NULLS FIRST], false, 0
               :  +- Exchange hashpartitioning(sr_returned_date_sk#49L, 1024)
               :     +- *(13) Project [ss_sold_date_sk#29L, sr_returned_date_sk#49L, s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75]
               :        +- *(13) SortMergeJoin [ss_sold_date_sk#29L], [d_date_sk#79L], Inner
               :           :- *(10) Sort [ss_sold_date_sk#29L ASC NULLS FIRST], false, 0
               :           :  +- Exchange hashpartitioning(ss_sold_date_sk#29L, 1024)
               :           :     +- *(9) Project [ss_sold_date_sk#29L, sr_returned_date_sk#49L, s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75]
               :           :        +- *(9) SortMergeJoin [ss_store_sk#13L], [s_store_sk#50L], Inner
               :           :           :- *(6) Sort [ss_store_sk#13L ASC NULLS FIRST], false, 0
               :           :           :  +- Exchange hashpartitioning(ss_store_sk#13L, 1024)
               :           :           :     +- *(5) Project [ss_store_sk#13L, ss_sold_date_sk#29L, sr_returned_date_sk#49L]
               :           :           :        +- *(5) SortMergeJoin [ss_ticket_number#15L, ss_item_sk#8L, ss_customer_sk#9L], [sr_ticket_number#38L, sr_item_sk#31L, sr_customer_sk#32L], Inner
               :           :           :           :- *(2) Sort [ss_ticket_number#15L ASC NULLS FIRST, ss_item_sk#8L ASC NULLS FIRST, ss_customer_sk#9L ASC NULLS FIRST], false, 0
               :           :           :           :  +- Exchange hashpartitioning(ss_ticket_number#15L, ss_item_sk#8L, ss_customer_sk#9L, 1024)
               :           :           :           :     +- *(1) Project [ss_item_sk#8L, ss_customer_sk#9L, ss_store_sk#13L, ss_ticket_number#15L, ss_sold_date_sk#29L]
               :           :           :           :        +- *(1) Filter (((isnotnull(ss_ticket_number#15L) && isnotnull(ss_customer_sk#9L)) && isnotnull(ss_item_sk#8L)) && isnotnull(ss_store_sk#13L))
               :           :           :           :           +- *(1) FileScan parquet tpcds_wangfei_100g.store_sales[ss_item_sk#8L,ss_customer_sk#9L,ss_store_sk#13L,ss_ticket_number#15L,ss_sold_date_sk#29L] Batched: true, Format: Parquet, Location: PrunedInMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/store_sales/ss_sold_date_..., PartitionCount: 1823, PartitionFilters: [isnotnull(ss_sold_date_sk#29L)], PushedFilters: [IsNotNull(ss_ticket_number), IsNotNull(ss_customer_sk), IsNotNull(ss_item_sk), IsNotNull(ss_stor..., ReadSchema: struct<ss_item_sk:bigint,ss_customer_sk:bigint,ss_store_sk:bigint,ss_ticket_number:bigint>
               :           :           :           +- *(4) Sort [sr_ticket_number#38L ASC NULLS FIRST, sr_item_sk#31L ASC NULLS FIRST, sr_customer_sk#32L ASC NULLS FIRST], false, 0
               :           :           :              +- Exchange hashpartitioning(sr_ticket_number#38L, sr_item_sk#31L, sr_customer_sk#32L, 1024)
               :           :           :                 +- *(3) Project [sr_item_sk#31L, sr_customer_sk#32L, sr_ticket_number#38L, sr_returned_date_sk#49L]
               :           :           :                    +- *(3) Filter ((isnotnull(sr_item_sk#31L) && isnotnull(sr_customer_sk#32L)) && isnotnull(sr_ticket_number#38L))
               :           :           :                       +- *(3) FileScan parquet tpcds_wangfei_100g.store_returns[sr_item_sk#31L,sr_customer_sk#32L,sr_ticket_number#38L,sr_returned_date_sk#49L] Batched: true, Format: Parquet, Location: PrunedInMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/store_returns/sr_returned..., PartitionCount: 2003, PartitionFilters: [isnotnull(sr_returned_date_sk#49L)], PushedFilters: [IsNotNull(sr_item_sk), IsNotNull(sr_customer_sk), IsNotNull(sr_ticket_number)], ReadSchema: struct<sr_item_sk:bigint,sr_customer_sk:bigint,sr_ticket_number:bigint>
               :           :           +- *(8) Sort [s_store_sk#50L ASC NULLS FIRST], false, 0
               :           :              +- Exchange hashpartitioning(s_store_sk#50L, 1024)
               :           :                 +- *(7) Project [s_store_sk#50L, s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75]
               :           :                    +- *(7) Filter isnotnull(s_store_sk#50L)
               :           :                       +- *(7) FileScan parquet tpcds_wangfei_100g.store[s_store_sk#50L,s_store_name#55,s_company_id#66,s_street_number#68,s_street_name#69,s_street_type#70,s_suite_number#71,s_city#72,s_county#73,s_state#74,s_zip#75] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/store], PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:bigint,s_store_name:string,s_company_id:int,s_street_number:string,s_street_nam...
               :           +- *(12) Sort [d_date_sk#79L ASC NULLS FIRST], false, 0
               :              +- Exchange hashpartitioning(d_date_sk#79L, 1024)
               :                 +- *(11) Project [d_date_sk#79L]
               :                    +- *(11) Filter isnotnull(d_date_sk#79L)
               :                       +- *(11) FileScan parquet tpcds_wangfei_100g.date_dim[d_date_sk#79L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/date_dim], PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:bigint>
               +- *(16) Sort [d_date_sk#107L ASC NULLS FIRST], false, 0
                  +- Exchange hashpartitioning(d_date_sk#107L, 1024)
                     +- *(15) Project [d_date_sk#107L]
                        +- *(15) Filter ((((isnotnull(d_year#113L) && isnotnull(d_moy#115L)) && (d_year#113L = 2000)) && (d_moy#115L = 9)) && isnotnull(d_date_sk#107L))
                           +- *(15) FileScan parquet tpcds_wangfei_100g.date_dim[d_date_sk#107L,d_year#113L,d_moy#115L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/date_dim], PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2000), EqualTo(d_moy,9), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:bigint,d_year:bigint,d_moy:bigint>
Time taken: 33.931 seconds, Fetched 1 row(s)

```



优化之后:

```scala
== Physical Plan ==
TakeOrderedAndProject(limit=100, orderBy=[s_store_name#55 ASC NULLS FIRST,s_company_id#66 ASC NULLS FIRST,s_street_number#68 ASC NULLS FIRST,s_street_name#69 ASC NULLS FIRST,s_street_type#70 ASC NULLS FIRST,s_suite_number#71 ASC NULLS FIRST,s_city#72 ASC NULLS FIRST,s_county#73 ASC NULLS FIRST,s_state#74 ASC NULLS FIRST,s_zip#75 ASC NULLS FIRST], output=[s_store_name#55,s_company_id#66,s_street_number#68,s_street_name#69,s_street_type#70,s_suite_number#71,s_city#72,s_county#73,s_state#74,s_zip#75,30days#0L,3160days#1L,6190days#2L,91120days#3L,dy120days#4L])
+- *(9) HashAggregate(keys=[s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75], functions=[sum(cast(CASE WHEN ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 30) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 30) && ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 60)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 60) && ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 90)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN (((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 90) && ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 120)) THEN 1 ELSE 0 END as bigint)), sum(cast(CASE WHEN ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 120) THEN 1 ELSE 0 END as bigint))])
   +- Exchange hashpartitioning(s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75, 1024)
      +- *(8) HashAggregate(keys=[s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75], functions=[partial_sum(cast(CASE WHEN ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 30) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 30) && ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 60)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 60) && ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 90)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN (((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 90) && ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) <= 120)) THEN 1 ELSE 0 END as bigint)), partial_sum(cast(CASE WHEN ((sr_returned_date_sk#49L - ss_sold_date_sk#29L) > 120) THEN 1 ELSE 0 END as bigint))])
         +- *(8) Project [ss_sold_date_sk#29L, sr_returned_date_sk#49L, s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75]
            +- *(8) SortMergeJoin [ss_customer_sk#9L, ss_ticket_number#15L, ss_item_sk#8L], [sr_customer_sk#32L, sr_ticket_number#38L, sr_item_sk#31L], Inner
               :- *(4) Sort [ss_customer_sk#9L ASC NULLS FIRST, ss_ticket_number#15L ASC NULLS FIRST, ss_item_sk#8L ASC NULLS FIRST], false, 0
               :  +- Exchange hashpartitioning(ss_customer_sk#9L, ss_ticket_number#15L, ss_item_sk#8L, 1024)
               :     +- *(3) Project [ss_item_sk#8L, ss_customer_sk#9L, ss_ticket_number#15L, ss_sold_date_sk#29L, s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75]
               :        +- *(3) BroadcastHashJoin [ss_store_sk#13L], [s_store_sk#50L], Inner, BuildRight
               :           :- *(3) Project [ss_item_sk#8L, ss_customer_sk#9L, ss_store_sk#13L, ss_ticket_number#15L, ss_sold_date_sk#29L]
               :           :  +- *(3) BroadcastHashJoin [ss_sold_date_sk#29L], [d_date_sk#79L], Inner, BuildRight
               :           :     :- *(3) Project [ss_item_sk#8L, ss_customer_sk#9L, ss_store_sk#13L, ss_ticket_number#15L, ss_sold_date_sk#29L]
               :           :     :  +- *(3) Filter (((isnotnull(ss_item_sk#8L) && isnotnull(ss_customer_sk#9L)) && isnotnull(ss_ticket_number#15L)) && isnotnull(ss_store_sk#13L))
               :           :     :     +- *(3) FileScan parquet tpcds_wangfei_100g.store_sales[ss_item_sk#8L,ss_customer_sk#9L,ss_store_sk#13L,ss_ticket_number#15L,ss_sold_date_sk#29L] Batched: true, Format: Parquet, Location: PrunedInMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/store_sales/ss_sold_date_..., PartitionCount: 1823, PartitionFilters: [isnotnull(ss_sold_date_sk#29L)], PushedFilters: [IsNotNull(ss_item_sk), IsNotNull(ss_customer_sk), IsNotNull(ss_ticket_number), IsNotNull(ss_stor..., ReadSchema: struct<ss_item_sk:bigint,ss_customer_sk:bigint,ss_store_sk:bigint,ss_ticket_number:bigint>
               :           :     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
               :           :        +- *(1) Project [d_date_sk#79L]
               :           :           +- *(1) Filter isnotnull(d_date_sk#79L)
               :           :              +- *(1) FileScan parquet tpcds_wangfei_100g.date_dim[d_date_sk#79L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/date_dim], PartitionFilters: [], PushedFilters: [IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:bigint>
               :           +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
               :              +- *(2) Project [s_store_sk#50L, s_store_name#55, s_company_id#66, s_street_number#68, s_street_name#69, s_street_type#70, s_suite_number#71, s_city#72, s_county#73, s_state#74, s_zip#75]
               :                 +- *(2) Filter isnotnull(s_store_sk#50L)
               :                    +- *(2) FileScan parquet tpcds_wangfei_100g.store[s_store_sk#50L,s_store_name#55,s_company_id#66,s_street_number#68,s_street_name#69,s_street_type#70,s_suite_number#71,s_city#72,s_county#73,s_state#74,s_zip#75] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/store], PartitionFilters: [], PushedFilters: [IsNotNull(s_store_sk)], ReadSchema: struct<s_store_sk:bigint,s_store_name:string,s_company_id:int,s_street_number:string,s_street_nam...
               +- *(7) Sort [sr_customer_sk#32L ASC NULLS FIRST, sr_ticket_number#38L ASC NULLS FIRST, sr_item_sk#31L ASC NULLS FIRST], false, 0
                  +- Exchange hashpartitioning(sr_customer_sk#32L, sr_ticket_number#38L, sr_item_sk#31L, 1024)
                     +- *(6) Project [sr_item_sk#31L, sr_customer_sk#32L, sr_ticket_number#38L, sr_returned_date_sk#49L]
                        +- *(6) BroadcastHashJoin [d_date_sk#107L], [sr_returned_date_sk#49L], Inner, BuildLeft
                           :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true]))
                           :  +- *(5) Project [d_date_sk#107L]
                           :     +- *(5) Filter ((((isnotnull(d_year#113L) && isnotnull(d_moy#115L)) && (d_year#113L = 2000)) && (d_moy#115L = 9)) && isnotnull(d_date_sk#107L))
                           :        +- *(5) FileScan parquet tpcds_wangfei_100g.date_dim[d_date_sk#107L,d_year#113L,d_moy#115L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/date_dim], PartitionFilters: [], PushedFilters: [IsNotNull(d_year), IsNotNull(d_moy), EqualTo(d_year,2000), EqualTo(d_moy,9), IsNotNull(d_date_sk)], ReadSchema: struct<d_date_sk:bigint,d_year:bigint,d_moy:bigint>
                           +- *(6) Project [sr_item_sk#31L, sr_customer_sk#32L, sr_ticket_number#38L, sr_returned_date_sk#49L]
                              +- *(6) Filter ((isnotnull(sr_item_sk#31L) && isnotnull(sr_customer_sk#32L)) && isnotnull(sr_ticket_number#38L))
                                 +- *(6) FileScan parquet tpcds_wangfei_100g.store_returns[sr_item_sk#31L,sr_customer_sk#32L,sr_ticket_number#38L,sr_returned_date_sk#49L] Batched: true, Format: Parquet, Location: PrunedInMemoryFileIndex[hdfs://dev/user/warehouse/tpcds_wangfei_100g.db/store_returns/sr_returned..., PartitionCount: 2003, PartitionFilters: [isnotnull(sr_returned_date_sk#49L)], PushedFilters: [IsNotNull(sr_item_sk), IsNotNull(sr_customer_sk), IsNotNull(sr_ticket_number)], ReadSchema: struct<sr_item_sk:bigint,sr_customer_sk:bigint,sr_ticket_number:bigint>
Time taken: 31.264 seconds, Fetched 1 row(s)



```