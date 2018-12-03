# c前言

kyubbi是姚总写的一个多租户插件，github地址为https://github.com/yaooqinn/kyuubi。

这个插件的作用是为了让同一个的多个session之间可以共用SparkContext。个人认为。kyubbi是一个类似于thrift server的服务，通过jdbc链接链接kyubbi，然后发送查询，然后kyubbi可以启动一些资源来对这个查询进行计算，然后将计算结果返还给这个jdbc链接。因此，使用kyubbi的好处就是省去了一些重新打包到yarn的过程，使得一些简短的查询更快的相应。而且kyubbi做到了session级别的参数控制，更加的高效和利用资源。

看kyubbi的代码目录结构，其实是和thrift server很像的。



apache thrift相关文档: https://www.ibm.com/developerworks/cn/java/j-lo-apachethrift/index.html

## 执行流程



