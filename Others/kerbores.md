# Kerberos快速生成Keytab

1. 连接kdc
```bash
#在本机
kadmin.local
#不在本机 
kadmin -p hadoop/admin
hadoop #输入密码
```
2. 创建Principal
```bash
kadmin:  addprinc logstash
123456
123456 #输入任意密码
```
3. 生成keytab
  前一个参数为生成的keytab文件名，后一个为上一步创建的principal
```
kadmin:  xst -k logstash.keytab logstash
```
4. 写用于kafka的jaas文件
```
KafkaClient {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
	# keytab路径
  keyTab="/path/to/logstash.keytab"
  storeKey=true
  useTicketCache=false
  serviceName="kafka"
  # principal信息
  principal="logstash@BDMS.163.COM";
};
```



# Kerbores 重启

systemctl status servicename



systemctl restart krb5kdc

systemctl restart kadmin

