---
layout: post
category: spark
tagline: ""
summary: spark统一内存管理是spark1.6.0的新特性，是对shuffle memory 和 storage memory 进行统一的管理，打破了以往的参数限制。
title: spark统一内存管理
tags: [spark]
---
{% include JB/setup %}
### Background ###
{{ page.summary }}



## 非统一内存管理 ##

spark在1.6 之前都是非统一内存管理，通过设置`spark.shuffle.memoryFraction` 和 `spark.storage.memoryFraction`来设置shuffle 和storage的memory 大小。看下`StaticMemoryManager`的获得最大shuffle和storage memory的函数。

```
private def getMaxStorageMemory(conf: SparkConf): Long = {
  val systemMaxMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
  val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
  val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
  (systemMaxMemory * memoryFraction * safetyFraction).toLong
}

/**
 * Return the total amount of memory available for the execution region, in bytes.
 */
private def getMaxExecutionMemory(conf: SparkConf): Long = {
  val systemMaxMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
...
  val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
  val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
  (systemMaxMemory * memoryFraction * safetyFraction).toLong
}
```
可以看出，`systemMaxMemory`是通过参数`spark.testing.memory`来获得，如果这个参数没有设置，就取虚拟机内存，然后shuffle 和 storage都有安全系数，最后可用的最大内存都是：系统最大内存\*比例系数\*安全系数。



## 统一内存管理 ##

spark 1.6.0 出现了统一内存管理，是打破了shuffle 内存和storage内存的静态限制。通俗的描述，就是如果storage内存不够，而shuffle内存剩余就能借内存，如果shuffle内存不足，此时如果storage已经超出了`storageRegionSize`，那么就驱逐当前使用storage内存-`storageRegionSize`，如果storage 使用没有超过`storageRegionSize`，那么则把它剩余的都可以借给shuffle使用。

```
  private def getMaxMemory(conf: SparkConf): Long = {
    val systemMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    val reservedMemory = conf.getLong("spark.testing.reservedMemory",
      if (conf.contains("spark.testing")) 0 else RESERVED_SYSTEM_MEMORY_BYTES)
    val minSystemMemory = (reservedMemory * 1.5).ceil.toLong
    if (systemMemory < minSystemMemory) {
      throw new IllegalArgumentException(s"System memory $systemMemory must " +
        s"be at least $minSystemMemory. Please increase heap size using the --driver-memory " +
        s"option or spark.driver.memory in Spark configuration.")
    }
    // SPARK-12759 Check executor memory to fail fast if memory is insufficient
    if (conf.contains("spark.executor.memory")) {
      val executorMemory = conf.getSizeAsBytes("spark.executor.memory")
      if (executorMemory < minSystemMemory) {
        throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
          s"$minSystemMemory. Please increase executor memory using the " +
          s"--executor-memory option or spark.executor.memory in Spark configuration.")
      }
    }
    val usableMemory = systemMemory - reservedMemory
    val memoryFraction = conf.getDouble("spark.memory.fraction", 0.6)
    (usableMemory * memoryFraction).toLong
  }
```
这个是统一内存管理的获得最大内存的函数，因为shuffle和storage是统一管理的，所以只有一个获得统一最大内存的函数。`usableMemory = systemMemory - reservedMemory`.

最大内存=`usableMemory * memoryFraction`.

## 统一内存管理的使用##

`UnifiedMemoryManager`是在一个静态类里面的`apply`方法调用的。

```
def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
  val maxMemory = getMaxMemory(conf)
  new UnifiedMemoryManager(
    conf,
    maxHeapMemory = maxMemory,
    onHeapStorageRegionSize =
      (maxMemory * conf.getDouble("spark.memory.storageFraction", 0.5)).toLong,
    numCores = numCores)
}
```

然后通过 find Uages 找到是在 `sparkEnv`里面调用。

```
val memoryManager: MemoryManager =
  if (useLegacyMemoryManager) {
    new StaticMemoryManager(conf, numUsableCores)
  } else {
    UnifiedMemoryManager(conf, numUsableCores)
  }
```

是通过判断参数，判断是使用统一内存管理还是非内存管理。

然后通过查看usages 发现是在 `CoarseGrainedExecutorBackEnd` 和 `MesosExecutorBackEnd`里面调用的，所以是每个executor都有一个统一内存管理的实例(...很显然，逻辑也是这样)。