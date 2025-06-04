package org.apache.spark.sql.execution.datasources.milvus

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.Test

class MilvusSparkTest {

  @Test
  def milvusConnectorTest(): Unit = {
    val milvusInstanceUtil = new MilvusInstanceUtil()
    println("create collection and insert data into collection: " + MilvusInstanceUtil.COLLECTION_NAME)
    milvusInstanceUtil.createCollection()
    milvusInstanceUtil.insertCollection()
    milvusInstanceUtil.queryCollectionCount()
    milvusInstanceUtil.queryCollection()

    println("start read data from milvus：" + MilvusInstanceUtil.COLLECTION_NAME + " into DataFrame...")
    val spark = SparkSession.builder().appName("milvus2milvus").master("local").getOrCreate()
    val df = spark.read
      .format("org.apache.spark.sql.execution.datasources.milvus.MilvusDataSource")
      .option("uri", MilvusInstanceUtil.uri)
      .option("collection", MilvusInstanceUtil.COLLECTION_NAME)
      .load()
    df.show(5, false)
    println(df.count())


    println("create collection: " + MilvusInstanceUtil.COLLECTION_NAME2)
    milvusInstanceUtil.createCollection2()
    milvusInstanceUtil.queryCollectionCount2()
    println("start write data from DataFrame into milvus collection: " + MilvusInstanceUtil.COLLECTION_NAME2 + "...")
    df.write.format("org.apache.spark.sql.execution.datasources.milvus.MilvusDataSource")
      .option("uri", MilvusInstanceUtil.uri)
      .option("collection", MilvusInstanceUtil.COLLECTION_NAME2)
      .mode(SaveMode.Append)
      .save()
    milvusInstanceUtil.queryCollectionCount2()
    milvusInstanceUtil.queryCollection2()

    milvusInstanceUtil.dropCollection()
    milvusInstanceUtil.dropCollection2()
    milvusInstanceUtil.close()
    spark.close()
  }
}
