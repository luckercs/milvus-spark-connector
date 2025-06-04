package org.apache.spark.sql.execution.datasources.milvus

import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class MilvusDataSource extends DataSourceRegister with RelationProvider with CreatableRelationProvider {

  override def shortName() = "milvus"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = MilvusRelation(
    sqlContext,
    parameters.getOrElse("uri", throw new IllegalArgumentException("host parameter is required")),
    parameters.getOrElse("token", "root:Milvus"),
    parameters.getOrElse("database", "default"),
    parameters.getOrElse("collection", throw new IllegalArgumentException("collection parameter is required")),
    parameters.getOrElse("batchsize", "1000")
  )

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    if (mode.equals(SaveMode.Overwrite) || mode.equals(SaveMode.ErrorIfExists)) {
      throw new RuntimeException("Only Append mode is supported")
    }
    val milvusRelation = MilvusRelation(
      sqlContext,
      parameters.getOrElse("uri", throw new IllegalArgumentException("uri parameter is required")),
      parameters.getOrElse("token", "root:Milvus"),
      parameters.getOrElse("database", "default"),
      parameters.getOrElse("collection", throw new IllegalArgumentException("collection parameter is required")),
      parameters.getOrElse("batchsize", "1000")
    )
    milvusRelation.insert(data, true)
    milvusRelation
  }
}
