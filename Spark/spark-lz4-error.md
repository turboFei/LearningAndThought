## LZ4错误

### 问题描述：

任务执行过程中， 出现Caused by: java.io.IOException: Stream is corrupted或者Caused by: java.io.IOException: FAILED_TO_UNCOMPRESS(5)异常（ shuffle压缩算法lz4和snappy相关异常） 

部分异常日志：

lz4：



```java
//代码占位符
Job aborted due to stage failure: Task 1757 in stage 4.0 failed 4 times, most recent failure: Lost task 1757.3 in stage 4.0 (TID 17639, hadoop1610.lt.163.org, executor 396): java.io.IOException: Stream is corrupted

at org.apache.spark.io.LZ4BlockInputStream.refill(LZ4BlockInputStream.java:163)

at org.apache.spark.io.LZ4BlockInputStream.read(LZ4BlockInputStream.java:125)

at java.io.BufferedInputStream.fill(BufferedInputStream.java:235)

at java.io.BufferedInputStream.read(BufferedInputStream.java:254)

at java.io.DataInputStream.readInt(DataInputStream.java:387)

at org.apache.spark.sql.execution.UnsafeRowSerializerInstance$$anon$3$$anon$1.readSize(UnsafeRowSerializer.scala:113)

at org.apache.spark.sql.execution.UnsafeRowSerializerInstance$$anon$3$$anon$1.<init>(UnsafeRowSerializer.scala:120)

at org.apache.spark.sql.execution.UnsafeRowSerializerInstance$$anon$3.asKeyValueIterator(UnsafeRowSerializer.scala:110)

at org.apache.spark.shuffle.BlockStoreShuffleReader$$anonfun$3.apply(BlockStoreShuffleReader.scala:66)

at org.apache.spark.shuffle.BlockStoreShuffleReader$$anonfun$3.apply(BlockStoreShuffleReader.scala:62)

at scala.collection.Iterator$$anon$12.nextCur(Iterator.scala:434)

at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:440)

at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:408)

at org.apache.spark.util.CompletionIterator.hasNext(CompletionIterator.scala:32)

at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:39)

at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:408)

at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIterator.sort_addToSorter$(Unknown Source)

at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIterator.processNext(Unknown Source)

at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)

at org.apache.spark.sql.execution.WholeStageCodegenExec$$anonfun$8$$anon$1.hasNext(WholeStageCodegenExec.scala:377)

at org.apache.spark.sql.execution.RowIteratorFromScala.advanceNext(RowIterator.scala:83)

at org.apache.spark.sql.execution.joins.SortMergeJoinScanner.advancedStreamed(SortMergeJoinExec.scala:724)

at org.apache.spark.sql.execution.joins.SortMergeJoinScanner.findNextOuterJoinRows(SortMergeJoinExec.scala:685)

at org.apache.spark.sql.execution.joins.OneSideOuterIterator.advanceStream(SortMergeJoinExec.scala:847)

at org.apache.spark.sql.execution.joins.OneSideOuterIterator.advanceNext(SortMergeJoinExec.scala:880)

at org.apache.spark.sql.execution.RowIteratorToScala.hasNext(RowIterator.scala:68)

at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIterator.processNext(Unknown Source)

at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)

at org.apache.spark.sql.execution.WholeStageCodegenExec$$anonfun$8$$anon$1.hasNext(WholeStageCodegenExec.scala:377)

at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:408)

at org.apache.spark.shuffle.sort.UnsafeShuffleWriter.write(UnsafeShuffleWriter.java:166)

at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:96)

at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:53)

at org.apache.spark.scheduler.Task.run(Task.scala:99)

at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:322)

at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)

at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)

at java.lang.Thread.run(Thread.java:745)
```



snappy:

```java
18/09/13 08:10:08 INFO client.TransportClientFactory: Successfully created connection to hadoop2997.lt.163.org/10.130.127.55:7337 after 0 ms (0 ms spent in bootstraps) 18/09/13 08:10:08 ERROR util.Utils: Aborting task java.io.IOException: FAILED_TO_UNCOMPRESS(5) 
at org.xerial.snappy.SnappyNative.throw_error(SnappyNative.java:98)
at org.xerial.snappy.SnappyNative.rawUncompress(Native Method) 
at org.xerial.snappy.Snappy.rawUncompress(Snappy.java:474) 
at org.xerial.snappy.Snappy.uncompress(Snappy.java:513) 
at org.xerial.snappy.SnappyInputStream.readFully(SnappyInputStream.java:147) 
at org.xerial.snappy.SnappyInputStream.readHeader(SnappyInputStream.java:99) 
at org.xerial.snappy.SnappyInputStream.<init>(SnappyInputStream.java:59) 
at org.apache.spark.io.SnappyCompressionCodec.compressedInputStream(CompressionCodec.scala:158) 
at org.apache.spark.serializer.SerializerManager.wrapForCompression(SerializerManager.scala:164) 
at org.apache.spark.serializer.SerializerManager.wrapStream(SerializerManager.scala:125) 
at org.apache.spark.shuffle.BlockStoreShuffleReader$$anonfun$2.apply(BlockStoreShuffleReader.scala:50) 
at org.apache.spark.shuffle.BlockStoreShuffleReader$$anonfun$2.apply(BlockStoreShuffleReader.scala:50) 
at org.apache.spark.storage.ShuffleBlockFetcherIterator.next(ShuffleBlockFetcherIterator.scala:363) 
at org.apache.spark.storage.ShuffleBlockFetcherIterator.next(ShuffleBlockFetcherIterator.scala:58) 
at scala.collection.Iterator$$anon$12.nextCur(Iterator.scala:434) 
at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:440) 
at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:408) 
at org.apache.spark.util.CompletionIterator.hasNext(CompletionIterator.scala:32) 
at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:39) at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:408) 
at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIterator.sort_addToSorter$(Unknown Source) 
at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIterator.processNext(Unknown Source) 
at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43) 
at org.apache.spark.sql.execution.WholeStageCodegenExec$$anonfun$8$$anon$1.hasNext(WholeStageCodegenExec.scala:377) 
at org.apache.spark.sql.execution.datasources.FileFormatWriter$DynamicPartitionWriteTask.execute(FileFormatWriter.scala:364) 
at org.apache.spark.sql.execution.datasources.FileFormatWriter$$anonfun$org$apache$spark$sql$execution$datasources$FileFormatWriter$$executeTask$3.apply(FileFormatWriter.scala:190) 
at org.apache.spark.sql.execution.datasources.FileFormatWriter$$anonfun$org$apache$spark$sql$execution$datasources$FileFormatWriter$$executeTask$3.apply(FileFormatWriter.scala:188) 
at org.apache.spark.util.Utils$.tryWithSafeFinallyAndFailureCallbacks(Utils.scala:1353) 
at org.apache.spark.sql.execution.datasources.FileFormatWriter$.org$apache$spark$sql$execution$datasources$FileFormatWriter$$executeTask(FileFormatWriter.scala:193) 
at org.apache.spark.sql.execution.datasources.FileFormatWriter$$anonfun$write$1$$anonfun$3.apply(FileFormatWriter.scala:129) 
at org.apache.spark.sql.execution.datasources.FileFormatWriter$$anonfun$write$1$$anonfun$3.apply(FileFormatWriter.scala:128) 
at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:87) 
at org.apache.spark.scheduler.Task.run(Task.scala:99) 
at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:325) 
at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145) 
at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615) 
at java.lang.Thread.run(Thread.java:745)
```



### 分析

lz4是一种压缩方式。 spark.io.compression.codec 默认为lz4，是用来压缩一些中间数据，比如RDD分为，event log， broadcast变量或者是shuffle outputs。默认是lz4，支持snappy，zstf，lzf等等。

通过看日志，可以看出是在shuffle read 的时候，通过ShuffleBlockFetcher拉取数据。

在unsafeRowSerializer中，数据是这样的，一个length,然后后面是数据。

这里是在读数据的时候，读了一个length，比如10000，但是去读数据，发现后面长度不到10000，这里就报I/O异常。



在 unsafeRowSerializer.deserializeStream方法中，askeyValueIterator中。

将din作为输入构建Iterator[(length,data)].

 din怎么来的？

在shuffleBlockReader中，会拉取map端partitiondIndexFile中，所有相应分区的数据。

然后，每个分区的数据是对应一个din。

这里有几个参数。



| 参数 | 默认值 | 说明 |
| ------------------------------- | ------------ | ------------------------------------------------------------ |
| `spark.reducer.maxSizeInFlight` | 48m          | Maximum size of map outputs to fetch simultaneously from each reduce task, in MiB unless otherwise specified. Since each output requires us to create a buffer to receive it, this represents a fixed memory overhead per reduce task, so keep it small unless you have a large amount of memory. |
| `spark.reducer.maxReqsInFlight` | Int.MaxValue | This configuration limits the number of remote requests to fetch blocks at any given point. When the number of hosts in the cluster increase, it might lead to very large number of in-bound connections to one or more nodes, causing the workers to fail under load. By allowing it to limit the number of fetch requests, this scenario can be mitigated. |

maxSizeInFlight默认48M，这个参数是指建一个48m的缓冲区接收从map端拉取的数据。

```scala
val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
      serializerManager.wrapStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue),
      SparkEnv.get.conf.getBoolean("spark.shuffle.detectCorrupt", true))

    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }
```



然后，每个map段拉取的数据作为一个datainputStream？？

是的，所以是每个 map端的数据，需要转换为一个keyvalueIterator（Iterator[(0,row)]。

刚才有个参数叫做**detectCorrupt**，这个参数是做校验。

```scala
      // Only copy the stream if it's wrapped by compression or encryption, also the size of
          // block is small (the decompressed block is smaller than maxBytesInFlight)
          if (detectCorrupt && !input.eq(in) && size < maxBytesInFlight / 3) {
            val originalInput = input
            val out = new ChunkedByteBufferOutputStream(64 * 1024, ByteBuffer.allocate)
            try {
              // Decompress the whole block at once to detect any corruption, which could increase
              // the memory usage tne potential increase the chance of OOM.
              // TODO: manage the memory used here, and spill it into disk in case of OOM.
              Utils.copyStream(input, out)
              out.close()
              input = out.toChunkedByteBuffer.toInputStream(dispose = true)
            } catch {
              case e: IOException =>
                buf.release()
                if (buf.isInstanceOf[FileSegmentManagedBuffer]
                  || corruptedBlocks.contains(blockId)) {
                  throwFetchFailedException(blockId, address, e)
                } else {
                  logWarning(s"got an corrupted block $blockId from $address, fetch again", e)
                  corruptedBlocks += blockId
                  fetchRequests += FetchRequest(address, Array((blockId, size)))
                  result = null
                }
            } finally {
              // TODO: release the buf here to free memory earlier
              originalInput.close()
              in.close()
            }
          }
```

这个参数默认是true，且需要size小于三分之一maxBytesInflight才做校验，也就是默认16M。

如果数据量大， 就不会做校验，就可能会出现问题， 报上面的异常，Stream is corrupted。

校验如果出错会报[fetch failed](./spark-pr-outputcommitor.md)错误。

但是对较大的数据，是不可能做校验的，这样可能会造成OOM。但是不校验又可能会出现数据错误。

所以，如果用户能够防止运行时候的数据倾斜，会避免或者减少这个错误的出现频率。

## 一些想法

为什么spark在read阶段，校验失败的时候会报fetchfailed，还有什么时候报这个错误呢？

总共有三处抛出，除了校验失败，另外两处为：

- local shuffle block出现IOException
- 还有就是Error occurred while fetching local blocks 和Failed to get block(s) from host:port



但是，这个地方，在readsize失败的时候，直接抛出IOException。

那么这个算是什么错误呢？

这个应该算是task失败，只是重新提交task。但是这大概率是din里面的数据都有问题，只是resubmittask，是很小几率让重新执行成功。我认为这里应该对这个exception进行catch，抛出fetchFailed。

从而resubmitStage。



但是这终归是一种比较讨巧的方法，深层次的，造成这个错误的根本原因到底是什么呢？



### 相关JIRA:

https://issues.apache.org/jira/browse/SPARK-3958