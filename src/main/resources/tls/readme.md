mqttx.keystore当前只启用APP单向认证

1、创建keystore文件，有效期20年
keytool -genkey -alias mqttx -keyalg RSA -validity 7300 -keystore mqttx.keystore

2、查看keystore文件
keytool -list -v -keystore mqttx.keystore -storepass 123456

多个服务器证书导入keystore操作指导
1.创建一个空的Keystore文件：在命令行窗口中输入以下命令，创建一个空的Keystore文件：
keytool -genkey -alias server-alias -keyalg RSA -keystore server.keystore

其中，-alias参数指定别名，-keyalg参数指定密钥算法，-keystore参数指定Keystore文件路径和名称。

2. 分别将每个服务端证书导入到Keystore中：在命令行窗口中输入以下命令，将每个服务端证书导入到Keystore中：
   keytool -import -alias server1-alias -file server1.crt -keystore server.keystore
   keytool -import -alias server2-alias -file server2.crt -keystore server.keystore
   keytool -import -alias server3-alias -file server3.crt -keystore server.keystore

其中，-alias参数指定别名，-file参数指定要导入的证书文件路径和名称，-keystore参数指定Keystore文件路径和名称。

3. 分别将每个服务端私钥导入到Keystore中：在命令行窗口中输入以下命令，将每个服务端私钥导入到Keystore中：
   keytool -importkey -alias server1-alias -file server1.key -keypass key-password -keystore server.keystore
   keytool -importkey -alias server2-alias -file server2.key -keypass key-password -keystore server.keystore
   keytool -importkey -alias server3-alias -file server3.key -keypass key-password -keystore server.keystore

其中，-alias参数指定别名，-file参数指定要导入的私钥文件路径和名称，-keypass参数指定私钥密码，-keystore参数指定Keystore文件路径和名称。
完成以上步骤后，多个服务端证书就已经被打包到一个Keystore文件中了。在服务端程序中，可以使用Keystore文件中的私钥和数字证书来进行数字签名和验证。