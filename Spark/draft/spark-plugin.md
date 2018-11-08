在spark中如果使用自己的组件。

```
<repository>
  <id>SparkPackagesRepo</id>
  <url>http://dl.bintray.com/spark-packages/maven</url>
  <releases>
    <enabled>true</enabled>
  </releases>
  <snapshots>
    <enabled>false</enabled>
  </snapshots>
</repository>
```

添加这个库

```
<spark.authorizer.version>1.1.3.spark2.1</spark.authorizer.version>
<spark.authorizer.scope>compile</spark.authorizer.scope>
```

添加scope和version。

