## Spark todo

- 自动化cache
- shuffle service
  - https://databricks.com/session/sos-optimizing-shuffle-i-o
- 小文件
- 数据倾斜
- AE 包括较老版本的ae
  - 小文件
  - 数据倾斜
- kyubbi cluster zookeeper spark context
  - 每个用户的session共用spark context
  - 使用zk，封装一个kyubby组件来保证，加入cache
  - 添加context的clean机制, idle, closed
- udf check
  - gitlab 选择，公司还是自己搭建服务
  - git repo设计    com.ne.kaola com.ne.music 如何保护隐私
  - jenkins 选择，公司还是自己搭建
  - jenkins插件
  - 静态代码检查











































![](../../imgs/plan/plan-q4.jpeg)