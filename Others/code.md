# String split

 特殊字符split时需要转义

```scala
scala> "org.apache.spark.sql.hive.orc".split(".")
res42: Array[String] = Array()

scala> "org.apache.spark.sql.hive.orc".split("\\.")
res43: Array[String] = Array(org, apache, spark, sql, hive, orc)

scala> "org|apache|spark|sql|hive|orc".split("|")
res44: Array[String] = Array(o, r, g, |, a, p, a, c, h, e, |, s, p, a, r, k, |, s, q, l, |, h, i, v, e, |, o, r, c)

scala> "org|apache|spark|sql|hive|orc".split("\\|")
res45: Array[String] = Array(org, apache, spark, sql, hive, orc)

```

