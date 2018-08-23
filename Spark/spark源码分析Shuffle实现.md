---
layout: post
category: spark
tagline: "spark Shuffle"
summary: spark shuff部分是spark源码的重要组成部分，shuffle发生在stage的交界处，对于spark的性能有重要影响，源码更新后，spark的shuffle机制也不一样，本文分析spark2.0的shuffle实现。
tags: [spark,shuffle]
---
{% include JB/setup %}
目录

* toc
{:toc}

### Background ###
{{ page.summary }}

本文基于spark2.0。

## Shuffle##

shuffle是Mapreduce框架中一个特定的phase，介于Map和Reduce之间。shuffle的英文意思是混洗，包含两个部分，shuffle write 和shuffle read。这里有一篇文章:[详细探究Spark的shuffle实现](http://jerryshao.me/architecture/2014/01/04/spark-shuffle-detail-investigation/)，这篇文章写于2014年，讲的是早期版本的shuffle实现。随着源码的更新，shuffle机制也做出了相应的优化，下面分析spark-2.0的shuffle机制。

`shuffleWriter`是一个抽象类，具体实现有三种，`BypassMergeSortShuffleWriter`,`sortShuffleWriter`,`UnsafeShuffleWriter`.



### BypassMergeSortShuffleWriter###

-_-,我先翻译下这个类开头给的注释，注释是很好的全局理解代码的工具，要好好理解。如下：

这个类实现了基于sort-shuffle的hash风格的shuffle fallback path（回退路径？怎么翻）。这个write路径把数据写到不同的文件里，每个文件对应一个reduce分区，然后把这些文件整合到一个单独的文件，这个文件的不同区域服务不同的reducer。数据不是缓存在内存中。这个类本质上和之前的`HashShuffleReader`，除了这个类的输出格式可以通过`org.apache.spark.shuffle.IndexShuffleBlockResolver`来调用。这个写路径对于有许多reduce分区的shuffle来说是不高效的，因为他同时打开很多serializers和文件流。因此只有在以下情况下才会选择这个路径：

1、没有排序  2、没有聚合操作  3、partition的数量小于bypassMergeThreshold 

这个代码曾经是ExternalSorter的一部分，但是为了减少代码复杂度就独立了出来。好，翻译结束。-_-



```
@Override
public void write(Iterator<Product2<K, V>> records) throws IOException {
  assert (partitionWriters == null);
  if (!records.hasNext()) {
    partitionLengths = new long[numPartitions];
    shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, null);
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
    return;
  }
  final SerializerInstance serInstance = serializer.newInstance();
  final long openStartTime = System.nanoTime();
  partitionWriters = new DiskBlockObjectWriter[numPartitions];
  for (int i = 0; i < numPartitions; i++) {
    final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
      blockManager.diskBlockManager().createTempShuffleBlock();
    final File file = tempShuffleBlockIdPlusFile._2();
    final BlockId blockId = tempShuffleBlockIdPlusFile._1();
    partitionWriters[i] =
      blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
  }
  // Creating the file to write to and creating a disk writer both involve interacting with
  // the disk, and can take a long time in aggregate when we open many files, so should be
  // included in the shuffle write time.
  writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

  while (records.hasNext()) {
    final Product2<K, V> record = records.next();
    final K key = record._1();
    partitionWriters[partitioner.getPartition(key)].write(key, record._2());
  }

  for (DiskBlockObjectWriter writer : partitionWriters) {
    writer.commitAndClose();
  }

  File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
  File tmp = Utils.tempFileWith(output);
  try {
    partitionLengths = writePartitionedFile(tmp);
    shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
  } finally {
    if (tmp.exists() && !tmp.delete()) {
      logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
    }
  }
  mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
}
```



前面都很好理解，就是根据key的哈希值写到不同的文件里面，然后就是`writePartitionedFile`和`writeIndexFileAndCommit`。



```
/**
 * Concatenate all of the per-partition files into a single combined file.
 *
 * @return array of lengths, in bytes, of each partition of the file (used by map output tracker).
 */
private long[] writePartitionedFile(File outputFile) throws IOException {
  // Track location of the partition starts in the output file
  final long[] lengths = new long[numPartitions];
  if (partitionWriters == null) {
    // We were passed an empty iterator
    return lengths;
  }

  final FileOutputStream out = new FileOutputStream(outputFile, true);
  final long writeStartTime = System.nanoTime();
  boolean threwException = true;
  try {
    for (int i = 0; i < numPartitions; i++) {
      final File file = partitionWriters[i].fileSegment().file();
      if (file.exists()) {
        final FileInputStream in = new FileInputStream(file);
        boolean copyThrewException = true;
        try {
          lengths[i] = Utils.copyStream(in, out, false, transferToEnabled);
          copyThrewException = false;
        } finally {
          Closeables.close(in, copyThrewException);
        }
        if (!file.delete()) {
          logger.error("Unable to delete file for partition {}", i);
        }
      }
    }
    threwException = false;
  } finally {
    Closeables.close(out, threwException);
    writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
  }
  partitionWriters = null;
  return lengths;
}
```



这个就是按顺序把之前写的分区文件里的数据合并到一个大文件里面，然后返回每个分区文件的长度。

```
/**
 * Write an index file with the offsets of each block, plus a final offset at the end for the
 * end of the output file. This will be used by getBlockData to figure out where each block
 * begins and ends.
 *
 * It will commit the data and index file as an atomic operation, use the existing ones, or
 * replace them with new ones.
 *
 * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
 * */
def writeIndexFileAndCommit(
    shuffleId: Int,
    mapId: Int,
    lengths: Array[Long],
    dataTmp: File): Unit = {
  val indexFile = getIndexFile(shuffleId, mapId)
  val indexTmp = Utils.tempFileWith(indexFile)
  try {
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))
    Utils.tryWithSafeFinally {
      // We take in lengths of each block, need to convert it to offsets.
      var offset = 0L
      out.writeLong(offset)
      for (length <- lengths) {
        offset += length
        out.writeLong(offset)
      }
    } {
      out.close()
    }

    val dataFile = getDataFile(shuffleId, mapId)
    // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
    // the following check and rename are atomic.
    synchronized {
      val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
      if (existingLengths != null) {
        // Another attempt for the same task has already written our map outputs successfully,
        // so just use the existing partition lengths and delete our temporary map outputs.
        System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
        if (dataTmp != null && dataTmp.exists()) {
          dataTmp.delete()
        }
        indexTmp.delete()
      } else {
        // This is the first successful attempt in writing the map outputs for this task,
        // so override any existing index and data files with the ones we wrote.
        if (indexFile.exists()) {
          indexFile.delete()
        }
        if (dataFile.exists()) {
          dataFile.delete()
        }
        if (!indexTmp.renameTo(indexFile)) {
          throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
        }
        if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
          throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
        }
      }
    }
  } finally {
    if (indexTmp.exists() && !indexTmp.delete()) {
      logError(s"Failed to delete temporary index file at ${indexTmp.getAbsolutePath}")
    }
  }
}
```



解释下这段代码，上来先写indexTmp，是把分区文件长度写进去，便于索引需要的那部分数据。然后就判断这个任务是不是第一次执行到这里，如果之前执行成功过，那就不用写了，直接用以前的结果就行。

如果是第一次执行到这里，那么就把之前的indexTmp重命名为indexFile，dataTmp重命名为dataFile然后返回。

这里要注意下，每个executor上面只有一个`IndexShuffleBlockResolver`，这个管理这个executor上所有的indexFile.

等这个indexFile也写好之后，就返回`mapStatus`。shuffleWrite就结束了。



### SortShuffleWriter ###



首先描述下大概。因为是sort，所以要排序，这里就用到了ExternalSoter这个数据结构。然后把要处理的数据全部插入到ExternalSorter里面，在插入的过程中是不排序的，就是插入，插入数据是(partitionId,key,value)。然后是调用` sorter.writePartitionedFile`,在这里会排序，会按照partitionId和key（或者key的hashcode）进行排序，其他的就和上面bypassShuffleWriter的差不多了，最后也是写到一个indexFile里面。返回mapStatus。



```
/** Write a bunch of records to this task's output */
override def write(records: Iterator[Product2[K, V]]): Unit = {
  sorter = if (dep.mapSideCombine) {
    require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
    new ExternalSorter[K, V, C](
      context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
  } else {
    // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
    // care whether the keys get sorted in each partition; that will be done on the reduce side
    // if the operation being run is sortByKey.
    new ExternalSorter[K, V, V](
      context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
  }
  sorter.insertAll(records)

  // Don't bother including the time to open the merged output file in the shuffle write time,
  // because it just opens a single file, so is typically too fast to measure accurately
  // (see SPARK-3570).
  val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
  val tmp = Utils.tempFileWith(output)
  try {
    val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
    val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
    shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
  } finally {
    if (tmp.exists() && !tmp.delete()) {
      logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
    }
  }
}
```



这里面ExternalSorter是核心。看它的源码，它存数据是使用的两种数据结构。`PartitionedAppendOnlyMap`、`PartitionedPairBuffer`，其中有聚合操作使用map，没有聚合操作使用buffer。PartitionedAppendOnlyMap 继承了SizeTrackingAppendOnlyMap 和WritablePartitionedPairCollection 。 其中SizeTrackingAppendOnlyMap是用于预测空间（SizeTracker），然后加存储数据（AppendOnlyMap）,然后WritablePartitionedPairCollection是用于插入数据时候插入partitionId（insert(partition: Int, key: K, value: V)）加上里面实现了对数据按照partitionId和Key排序的方法。

我主要是对AppendOnlyMap怎么存储数据比较感兴趣。看下AppendOnlyMap。

看源码，它存储数据是`private var data = new Array[AnyRef](2 * capacity)`,是使用数组存储的，key和value挨着，这样做是为了节省空间。

然后map的Update和changeValue函数是差不多的，只不过后者的changeValue是由计算函数计算的value，所以我们就看update方法。



```
/** Set the value for a key */
def update(key: K, value: V): Unit = {
  assert(!destroyed, destructionMessage)
  val k = key.asInstanceOf[AnyRef]
  if (k.eq(null)) {
    if (!haveNullValue) {
      incrementSize()
    }
    nullValue = value
    haveNullValue = true
    return
  }
  var pos = rehash(key.hashCode) & mask
  var i = 1
  while (true) {
    val curKey = data(2 * pos)
    if (curKey.eq(null)) {
      data(2 * pos) = k
      data(2 * pos + 1) = value.asInstanceOf[AnyRef]
      incrementSize()  // Since we added a new key
      return
    } else if (k.eq(curKey) || k.equals(curKey)) {
      data(2 * pos + 1) = value.asInstanceOf[AnyRef]
      return
    } else {
      val delta = i
      pos = (pos + delta) & mask
      i += 1
    }
  }
}
```



看源码可以看出，这里插入数据，采用的二次探测法。java.util.collection的HashMap在hash冲突时候采用的是链接法，而这里的二次探测法缺点就是删除元素时候比较复杂，不能简单的把数组中的相应位置设为null，这样就没办法查找元素，通常是把被删除的元素标记为已删除，但是又需要占据额外的空间。但是此处是appendOnlyMap，也就是只会追加（插入或者更新），不会删除，所以这个自定义的map更省内存。

然后这个AppendOnlyMap会在growMap的时候重新hash。在sorter.insertall时候是不排序的。

然后writePartitionedFile 里面调用`collection.destructiveSortedWritablePartitionedIterator(comparator)	`会对数据排序，之后就跟上一小节里面的writePartitionedFile差不多了，无非就是把内存里面的数据和spill的数据合并之后写入大文件里面，之后的writeIndexFile是一样的，就不细说。



### unsafeShuffleWriter ###

这里之所以叫作unsafe，是因为要操纵堆外内存，把数据写到堆外，堆外内存是不受jvm控制的，需要手动进行申请内存与释放内存空间，所以是unsafe的。

```
@Override
public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
  // Keep track of success so we know if we encountered an exception
  // We do this rather than a standard try/catch/re-throw to handle
  // generic throwables.
  boolean success = false;
  try {
    while (records.hasNext()) {
      insertRecordIntoSorter(records.next());
    }
    closeAndWriteOutput();
    success = true;
  } finally {
    if (sorter != null) {
      try {
        sorter.cleanupResources();
      } catch (Exception e) {
        // Only throw this error if we won't be masking another
        // error.
        if (success) {
          throw e;
        } else {
          logger.error("In addition to a failure during writing, we failed during " +
                       "cleanup.", e);
        }
      }
    }
  }
}
```

除了是写到堆外，其他应该跟sortShuffleWriter 差不多吧，懒得写了，以后发现有什么特别之处再补充。



### BlockStoreShuffleReader###

前面三个shuffleWriter，shuffle分为shuffleWriter和shuffleReader。shuffleReadr只有一个具体实现类就是BlockStoreShuffleReader。看开头注释为：读取（startPartition和endPartition）之间的partition的数据，从其他节点。

```
/** Read the combined key-values for this reduce task */
override def read(): Iterator[Product2[K, C]] = {
  val blockFetcherItr = new ShuffleBlockFetcherIterator(
    context,
    blockManager.shuffleClient,
    blockManager,
    mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
    // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
    SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
    SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue))

  // Wrap the streams for compression based on configuration
  val wrappedStreams = blockFetcherItr.map { case (blockId, inputStream) =>
    serializerManager.wrapForCompression(blockId, inputStream)
  }

  val serializerInstance = dep.serializer.newInstance()

  // Create a key/value iterator for each stream
  val recordIter = wrappedStreams.flatMap { wrappedStream =>
    // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
    // NextIterator. The NextIterator makes sure that close() is called on the
    // underlying InputStream when all records have been read.
    serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
  }

  // Update the context task metrics for each record read.
  val readMetrics = context.taskMetrics.createTempShuffleReadMetrics()
  val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
    recordIter.map { record =>
      readMetrics.incRecordsRead(1)
      record
    },
    context.taskMetrics().mergeShuffleReadMetrics())

  // An interruptible iterator must be used here in order to support task cancellation
  val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

  val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
    if (dep.mapSideCombine) {
      // We are reading values that are already combined
      val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
      dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
    } else {
      // We don't know the value type, but also don't care -- the dependency *should*
      // have made sure its compatible w/ this aggregator, which will convert the value
      // type to the combined type C
      val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
      dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
    }
  } else {
    require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
    interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
  }

  // Sort the output if there is a sort ordering defined.
  dep.keyOrdering match {
    case Some(keyOrd: Ordering[K]) =>
      // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
      // the ExternalSorter won't spill to disk.
      val sorter =
        new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
      sorter.insertAll(aggregatedIter)
      context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
      context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
      context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
      CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
    case None =>
      aggregatedIter
  }
}
```

首先是建立一个`ShuffleBlockFetcherIterator`，传入的参数有`mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition)`,这个是必须的，只取需要的partition的数据。

点进去ShuffleBlockFetcherIterator这个类，发现这个类会自动调用initialize()方法。

```
private[this] def initialize(): Unit = {
  // Add a task completion callback (called in both success case and failure case) to cleanup.
  context.addTaskCompletionListener(_ => cleanup())

  // Split local and remote blocks.
  val remoteRequests = splitLocalRemoteBlocks()
  // Add the remote requests into our queue in a random order
  fetchRequests ++= Utils.randomize(remoteRequests)
  assert ((0 == reqsInFlight) == (0 == bytesInFlight),
    "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
    ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

  // Send out initial requests for blocks, up to our maxBytesInFlight
  fetchUpToMaxBytes()

  val numFetches = remoteRequests.size - fetchRequests.size
  logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

  // Get Local Blocks
  fetchLocalBlocks()
  logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))
}
```

这个方法里面会`fetchUpToMaxBytes()`和`fetchLocalBlocks()`,一个是取远程数据一个是取本地数据。



```
private def fetchUpToMaxBytes(): Unit = {
  // Send fetch requests up to maxBytesInFlight
  while (fetchRequests.nonEmpty &&
    (bytesInFlight == 0 ||
      (reqsInFlight + 1 <= maxReqsInFlight &&
        bytesInFlight + fetchRequests.front.size <= maxBytesInFlight))) {
    sendRequest(fetchRequests.dequeue())
  }
}
```

这里会设置一个阈值，避免过度负载的。`sendRequest`来请求数据。

```
private[this] def sendRequest(req: FetchRequest) {
  logDebug("Sending request for %d blocks (%s) from %s".format(
    req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
  bytesInFlight += req.size
  reqsInFlight += 1

  // so we can look up the size of each blockID
  val sizeMap = req.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap
  val remainingBlocks = new HashSet[String]() ++= sizeMap.keys
  val blockIds = req.blocks.map(_._1.toString)

  val address = req.address
  shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
    new BlockFetchingListener {
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        // Only add the buffer to results queue if the iterator is not zombie,
        // i.e. cleanup() has not been called yet.
        ShuffleBlockFetcherIterator.this.synchronized {
          if (!isZombie) {
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            buf.retain()
            remainingBlocks -= blockId
            results.put(new SuccessFetchResult(BlockId(blockId), address, sizeMap(blockId), buf,
              remainingBlocks.isEmpty))
            logDebug("remainingBlocks: " + remainingBlocks)
          }
        }
        logTrace("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
      }

      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
        results.put(new FailureFetchResult(BlockId(blockId), address, e))
      }
    }
  )
}
```

后面一大堆代码，反正就是取数据吗，就不细看了。

取完数据之后，就通过dep.mapSideCombine判断是否在map端做了聚合操作，如果做了聚合操作，这里的(k,v)的v就是CompactBuffer类型，就调用combineCombinersByKey，如果在map端没有聚合，就还是value类型，就combineValuesByKey。

之后就判断是否定义了排序，如果需要排序就用ExternalSorter排序。

到这里shuffle过程就结束啦。



## 总结##

前两种shuffleWriter（UnsafeShuffleWriter没细看）里的shuffleWrite端最后得到的文件都只是一个IndexFile，这跟[早期的shuffle机制](http://jerryshao.me/architecture/2014/01/04/spark-shuffle-detail-investigation/)还是不一样的。