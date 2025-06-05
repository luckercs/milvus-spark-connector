# milvus-spark-connector

milvus-spark-connector, 支持 milvus 2.5.x 等版本

## (1) Requirements
- jdk8

## (2) Get Started

```shell

# (1) maven local install
# milvus-spark-connector-1.0.0.jar 可以在release发布页面下载获取
mvn install:install-file -Dfile=milvus-spark-connector-1.0.0.jar -DgroupId=com.luckercs -DartifactId=milvus-spark-connector -Dversion=1.0.0 -Dpackaging=jar

# (2) add maven dependency
<dependency>
    <groupId>com.luckercs</groupId>
    <artifactId>milvus-spark-connector</artifactId>
    <version>1.0.0</version>
</dependency>
<dependency>
    <groupId>io.milvus</groupId>
    <artifactId>milvus-sdk-java</artifactId>
    <version>2.5.9</version>
</dependency>

# (3) read and write milvus
val df = spark.read.format("milvus")
      .option("uri", "http://localhost:19530")
      .option("token", "root:Milvus")
      .option("database", "default")
      .option("collection", "test")
      .option("batchsize", 1000)
      .load()

df.write.format("milvus")
      .option("uri", "http://localhost:19530")
      .option("token", "root:Milvus")
      .option("database", "default")
      .option("collection", "test")
      .option("batchsize", 1000)
      .mode(SaveMode.Append)
      .save()
```
## (3) Thanks

如果这个项目对你有帮助，欢迎扫码打赏！

<img src="images/coffee.png" alt="coffee" width="200" height="200">

感谢你的慷慨解囊，你的支持是我前进的动力！
