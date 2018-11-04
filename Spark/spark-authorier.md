## 关于spark权限问题

spark本身不涉及权限。是因为加了spark-authorizer的规则，才使得进行权限校验。那么，为什么insert into的时候，会需要create权限。

是规则有问题么？

