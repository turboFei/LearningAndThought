## Pull Request

在源码中建立PR。

比如Spark，有时候能够打包并不代表里面的代码符合规范。

比如一些code style 和 unit test可能都有问题。

所以，在进行commit 之前或者是合并PR之前，使用./build/sbt ，然后test 可以发现里面的语法错误。

然后push到自己的repo里面触发CI，可以发现是否存在单元测试的错误。

因为有时候改了一段代码，会改变单元测试的逻辑。