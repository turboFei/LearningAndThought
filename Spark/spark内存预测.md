---
layout: post
category: spark
tagline: "sizeEstimate"
summary: spark是一个内存计算框架，因此内存是重要的资源，合理的使用的内存在spark应用在执行过程中非常重要。在使用内存的过程，spark会采用抽样的方法预测出所需要的内存，并预先分配内存。本文会就内存预测机制进行源码的解读。
tags: [spark,内存]
---
{% include JB/setup %}
目录

* toc
{:toc}



### Background ###
{{ page.summary }}



## sizeTracker ##



spark里面内存预测有一个trait，叫做` SizeTracker`，然后有一些类实现了它，比如PartitionedAppendOnlyMap、SizeTrackingAppendOnlyMap。

SizeTracker的estimateSize方法就是预测当前集合的size。



```
/**
 * Estimate the current size of the collection in bytes. O(1) time.
 */
def estimateSize(): Long = {
  assert(samples.nonEmpty)
  val extrapolatedDelta = bytesPerUpdate * (numUpdates - samples.last.numUpdates)
  (samples.last.size + extrapolatedDelta).toLong
}
```



其实这个sizeTracker类有四个方法，其他三个方法分别是`resetSamples`,`afterUpdate`,`takeSample`.看了下SizeTrackingAppendOnlyMap的流程，afterUpdata方法是在update或者changeValue之后会调用，其实updata和changeValue没有什么区别，只不过一个是直接更新k-v，另一个是使用一个函数计算后更新k-v。然后resetSamples是在growTable之后调用（SizeTrackingAppendOnlyMap的growTable就是空间翻一倍）。

看下sizeTracker里面的参数。



```
/**
 * Controls the base of the exponential which governs the rate of sampling.
 * E.g., a value of 2 would mean we sample at 1, 2, 4, 8, ... elements.
 */
private val SAMPLE_GROWTH_RATE = 1.1

/** Samples taken since last resetSamples(). Only the last two are kept for extrapolation. */
private val samples = new mutable.Queue[Sample]

/** The average number of bytes per update between our last two samples. */
private var bytesPerUpdate: Double = _

/** Total number of insertions and updates into the map since the last resetSamples(). */
private var numUpdates: Long = _

/** The value of 'numUpdates' at which we will take our next sample. */
private var nextSampleNum: Long = _
```



`SAMPLE_GROWTH_RATE`是一个斜率，代表下次抽样时候更新的次数应该是这次抽样更新次数的1.1倍，比如上次是更新10000次时候抽样，下次抽样就得是更新11000次时候再抽样，可以避免每次更新都抽样，减少抽样花销。`samples`是一个队列， 里面的类型是样例类`sample`。然后`bytesPerUpdate`是抽样之后得到区间增长量/个数增长量，就是一个斜率。然后`numUpdates`就是代表抽样集合里面元素个数，`nextSampleNum`代表下次要抽样的时候集合的个数，前面说过，就是此次抽样时候的个数\*1.1.



```
/**
 * Reset samples collected so far.
 * This should be called after the collection undergoes a dramatic change in size.
 */
protected def resetSamples(): Unit = {
  numUpdates = 1
  nextSampleNum = 1
  samples.clear()
  takeSample()
}
```



resetSamples会在每次翻倍增长后，重置抽样参数，没啥好说的。



```
/**
 * Callback to be invoked after every update.
 */
protected def afterUpdate(): Unit = {
  numUpdates += 1
  if (nextSampleNum == numUpdates) {
    takeSample()
  }
}
```



这个是每次更新后，都更新次数+1，然后当他等于下次抽样次数时候就进行抽样。



```
/**
 * Take a new sample of the current collection's size.
 */
private def takeSample(): Unit = {
  samples.enqueue(Sample(SizeEstimator.estimate(this), numUpdates))
  // Only use the last two samples to extrapolate
  if (samples.size > 2) {
    samples.dequeue()
  }
  val bytesDelta = samples.toList.reverse match {
    case latest :: previous :: tail =>
      (latest.size - previous.size).toDouble / (latest.numUpdates - previous.numUpdates)
    // If fewer than 2 samples, assume no change
    case _ => 0
  }
  bytesPerUpdate = math.max(0, bytesDelta)
  nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
}
```



抽样就是找出最近的两个sample，然后计算增长斜率，size增长量/num增长量，然后把下次抽样的次数\*1.1更新下。



```
/**
 * Estimate the current size of the collection in bytes. O(1) time.
 */
def estimateSize(): Long = {
  assert(samples.nonEmpty)
  val extrapolatedDelta = bytesPerUpdate * (numUpdates - samples.last.numUpdates)
  (samples.last.size + extrapolatedDelta).toLong
}
```



然后这个estimateSize 就是上次的size+增长率*增长量。增长率和size就是上次抽样得到的。

可以看到在takeSample方法里面加入队列时候size的预测用到了`SizeEstimator.estimate`.看下这个SizeEstimator类。



## SizeEstimator ##



看下这类的estimate方法。

```
private def estimate(obj: AnyRef, visited: IdentityHashMap[AnyRef, AnyRef]): Long = {
  val state = new SearchState(visited)
  state.enqueue(obj)
  while (!state.isFinished) {
    visitSingleObject(state.dequeue(), state)
  }
  state.size
}
```



这里主要是调用visitSingleObject。



```
private def visitSingleObject(obj: AnyRef, state: SearchState) {
  val cls = obj.getClass
  if (cls.isArray) {
    visitArray(obj, cls, state)
  } else if (cls.getName.startsWith("scala.reflect")) {
    // Many objects in the scala.reflect package reference global reflection objects which, in
    // turn, reference many other large global objects. Do nothing in this case.
  } else if (obj.isInstanceOf[ClassLoader] || obj.isInstanceOf[Class[_]]) {
    // Hadoop JobConfs created in the interpreter have a ClassLoader, which greatly confuses
    // the size estimator since it references the whole REPL. Do nothing in this case. In
    // general all ClassLoaders and Classes will be shared between objects anyway.
  } else {
    obj match {
      case s: KnownSizeEstimation =>
        state.size += s.estimatedSize
      case _ =>
        val classInfo = getClassInfo(cls)
        state.size += alignSize(classInfo.shellSize)
        for (field <- classInfo.pointerFields) {
          state.enqueue(field.get(obj))
        }
    }
  }
}
```



如果是Array类型，就visitArray。如果是scala.reflect开头的类，因为这个包里面涉及全局反射对象，因此涉及很多其他的大对象，所以这种对象不做任何操作。然后如果是classLoader类型，hadoop 作业在解释器中创建了classLoader，因为涉及整个REPL（读取-求值-处理-循环），所以很难处理。一般，所有classLoader和classes都是共享的。然后有的就是已经预测过的，直接读取。然后其他类型，就是拆解，拆成实际对象和引用，实际对象算出size相加，然后指针类型就把它指向的对象加入state队列，然后再进入while循环。直到state isFinished。

接下来看看visitArray.

```
// Estimate the size of arrays larger than ARRAY_SIZE_FOR_SAMPLING by sampling.
private val ARRAY_SIZE_FOR_SAMPLING = 400
private val ARRAY_SAMPLE_SIZE = 100 // should be lower than ARRAY_SIZE_FOR_SAMPLING

private def visitArray(array: AnyRef, arrayClass: Class[_], state: SearchState) {
  val length = ScalaRunTime.array_length(array)
  val elementClass = arrayClass.getComponentType()

  // Arrays have object header and length field which is an integer
  var arrSize: Long = alignSize(objectSize + INT_SIZE)

  if (elementClass.isPrimitive) {
    arrSize += alignSize(length.toLong * primitiveSize(elementClass))
    state.size += arrSize
  } else {
    arrSize += alignSize(length.toLong * pointerSize)
    state.size += arrSize

    if (length <= ARRAY_SIZE_FOR_SAMPLING) {
      var arrayIndex = 0
      while (arrayIndex < length) {
        state.enqueue(ScalaRunTime.array_apply(array, arrayIndex).asInstanceOf[AnyRef])
        arrayIndex += 1
      }
    } else {
      // Estimate the size of a large array by sampling elements without replacement.
      // To exclude the shared objects that the array elements may link, sample twice
      // and use the min one to calculate array size.
      val rand = new Random(42)
      val drawn = new OpenHashSet[Int](2 * ARRAY_SAMPLE_SIZE)
      val s1 = sampleArray(array, state, rand, drawn, length)
      val s2 = sampleArray(array, state, rand, drawn, length)
      val size = math.min(s1, s2)
      state.size += math.max(s1, s2) +
        (size * ((length - ARRAY_SAMPLE_SIZE) / (ARRAY_SAMPLE_SIZE))).toLong
    }
  }
}
```

这段代码，首先要把`Array 的object 头部,长度 filed`算进去，然后如果array里面的元素是基本类型，那么长度就固定，就可以直接算出来。

如果不是基本类型，`就有指向对象的引用？`所以代码里面先把length个指针占用的空间加上。

如果这时候数组长度，小于采样时候数组长度那个界限，就把数组里面引用指向的对象加入state队列，也就是小于界限就全部计算size。

如果数组长度大于采样时候数组长度的界限，就准备采样。然后采样两组，两组采样数据都是不重复的。计算公式如下:`math.max(s1, s2) + (math.min(s1, s2) * ((length - ARRAY_SAMPLE_SIZE) / (ARRAY_SAMPLE_SIZE)))`.

这个计算公式不知道有什么合理的地方，反正spark用这个公式，应该是有一定道理。

就是  `math.min(s1,s2)*(length-ARRAY_SAMPLE_SIZE)+abs(s1-s2)`，这应该是为了不让内存预估过大，以免占用太多，同时用一个小的增量对这个偏小的预估进行补偿。

