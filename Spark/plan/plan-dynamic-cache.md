## 自动cache

在spark sql 中，这个案例好像不存在，不存在需要执行多条语句来进行交叉cache的情况，肯定是一条语句一条来进行解析，执行。

其实cache运用的更多场景是进行图计算，机器学习，日志挖掘，这些迭代计算较多的。

引用一段知乎上面关于bigflow的一段文字

```
Q: Bigflow在Spark上时为什么会比Spark有性能提升，在Hadoop上跑时候为什么会比Hadoop快？
A: 主要来自以下方面：
1、全局数据流优化，Bigflow接口层面设计得更为精细，可以了解许多现有引擎无法得知的用户计算细节，完成更精细的全局数据流上的调整优化。
2、我们使用了底层引擎许多较为难用的高级feature，比大部分普通用户更知道分布式程序该如何优化，该使用哪些feature才能性能更高（在厂内Hadoop上已经使用了较多厂内Hadoop的高级优化，例如DAG引擎支持，动态缩减reduce并发数，在Spark上如何使用合适的partitioner来优化作业）。部分我们适用的场景下我们可能还会去修改引擎以适用我们的使用场景。这些使用场景对于普通的用户可能因为使用困难并不太会有人去用，但对于Bigflow可能会有较大的性能提升。（厂内Hadoop已有较多先例，例如MIMO支持，Broadcast支持等，Spark上目前和Baidu Spark组的同事一起讨论的MergeSort优化正在尝试向Spark社区提交代码）

作者：张云聪
链接：https://www.zhihu.com/question/263455441/answer/270498218
来源：知乎
著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。
```

难道第一条，bigflow获得了全局的dag，而不只是一个job的dag？？

没看到相关文档以及代码，无法判别。

[相关ppt](/Users/bbw/Documents/important/dynamic-cahce-ppt.pdf)

