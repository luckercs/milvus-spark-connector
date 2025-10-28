package org.apache.spark.sql.execution.datasources.milvus

import com.google.gson.{Gson, JsonArray, JsonObject}
import io.milvus.response.QueryResultsWrapper
import io.milvus.v2.common
import io.milvus.v2.service.collection.request.CreateCollectionReq
import io.milvus.v2.service.collection.response.DescribeCollectionResp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.MilvusUtil
import org.apache.spark.sql.execution.datasources.milvus.MilvusRelation.{Log, LogTag}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SQLContext}
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import java.util
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}
import scala.collection.SortedMap
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


case class MilvusRelation(
                           sqlContext: SQLContext,
                           uri: String,
                           token: String,
                           dbName: String,
                           collectionName: String,
                           batchsize: String
                         ) extends BaseRelation with TableScan {

  private val sparkSchemas: StructType = {
    milvusSchema2SparkSchema(getMilvusDesc.getCollectionSchema.getFieldSchemaList()).add("__partition", DataTypes.StringType, false)
  }

  private def getMilvusDesc(): DescribeCollectionResp = {
    val milvusUtil = new MilvusUtil(uri, token, dbName)
    if (!milvusUtil.hasCollection(collectionName)) {
      throw new Exception("milvus Collection " + collectionName + " does not exist in database " + dbName)
    }
    milvusUtil.getCollectionDesc(collectionName)
  }

  private def getMilvusPartitions(): util.List[String] = {
    val milvusUtil = new MilvusUtil(uri, token, dbName)
    milvusUtil.getCollectionPartitions(collectionName)
  }

  override def schema: StructType = {
    sparkSchemas
  }

  override def buildScan(): RDD[Row] = {
    val milvusDesc = getMilvusDesc()
    val milvusPartitions = getMilvusPartitions()
    val milvusUtil = new MilvusUtil(uri, token, dbName)
    val milvusClient = milvusUtil.getClient
    val rdds = new util.ArrayList[RDD[Row]]()

    milvusPartitions.asScala.foreach(partition => {
      val queryIterator = milvusUtil.queryCollection(milvusClient, collectionName, partition, batchsize.toLong)
      var flag = true
      while (flag) {
        val milvusRows = queryIterator.next()
        if (milvusRows.isEmpty()) {
          queryIterator.close()
          flag = false
        } else {
          Log.info(LogTag + "querying data from milvus with batchsize:" + batchsize)
          val batchRows = new util.ArrayList[Row]()
          for (milvusRow: QueryResultsWrapper.RowRecord <- milvusRows.asScala) {
            val milvusRowFieldValues: util.Map[String, AnyRef] = milvusRow.getFieldValues
            val milvusRowValuesSortByKey: Array[Object] = getValuesBySortedKey(milvusRowFieldValues)
            val milvusSparkData: Array[Object] = milvusData2SparkData(milvusDesc, milvusRowValuesSortByKey)
            val milvusSparkDataWithPartition = milvusSparkData :+ partition
            batchRows.add(RowFactory.create(milvusSparkDataWithPartition: _*))
          }
          rdds.add(sqlContext.sparkContext.parallelize(batchRows.asScala))
        }
      }
      queryIterator.close()
    })

    val resRDD = sqlContext.sparkContext.union(rdds.asScala)
    milvusUtil.closeClient(milvusClient)
    resRDD
  }


  def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.rdd.groupBy(row => row.getAs[String]("__partition")).foreachPartition(iter =>
      iter.foreach {
        case (partitionKey, rows: Iterable[Row]) =>
          val milvusDesc = getMilvusDesc()
          val milvusUtil = new MilvusUtil(uri, token, dbName)
          val milvusRows = new util.ArrayList[JsonObject]()
          rows.foreach(row => {
            milvusRows.add(SparkData2milvusData(milvusDesc, row))
            if (milvusRows.size() >= batchsize.toInt) {
              Log.info(LogTag + "insert data to milvus with batchsize:" + batchsize)
              milvusUtil.insertCollection(collectionName, partitionKey, milvusRows)
              milvusRows.clear()
            }
          })
          if (milvusRows.size() > 0) {
            milvusUtil.insertCollection(collectionName, partitionKey, milvusRows)
          }
      }
    )
  }


  private def milvusSchema2SparkSchema(milvusSchemas: util.List[CreateCollectionReq.FieldSchema]): StructType = {
    var sparkStructType = new StructType
    val milvusUtil = new MilvusUtil(uri, token, dbName)
    val funcOutputFieldList = milvusUtil.getCollectionFunctionOutputFieldList(collectionName)
    for (milvusFieldSchema <- milvusSchemas.asScala) {
      if (funcOutputFieldList.contains(milvusFieldSchema.getName)) {
        Log.info("skip milvus funcOutputField: " + milvusFieldSchema.getName)
      } else {
        milvusFieldSchema.getDataType match {
          case common.DataType.Bool => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.BooleanType, milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
          case common.DataType.Int8 => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.ByteType, milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
          case common.DataType.Int16 => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.ShortType, milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
          case common.DataType.Int32 => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.IntegerType, milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
          case common.DataType.Int64 => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.LongType, milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
          case common.DataType.Float => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.FloatType, milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
          case common.DataType.Double => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.DoubleType, milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
          case common.DataType.String => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.StringType, milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
          case common.DataType.VarChar => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.StringType, milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
          case common.DataType.JSON => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.StringType, milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
          case common.DataType.Array => {
            milvusFieldSchema.getElementType match {
              case common.DataType.Bool => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.createArrayType(DataTypes.BooleanType), milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
              case common.DataType.Int8 => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.createArrayType(DataTypes.ByteType), milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
              case common.DataType.Int16 => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.createArrayType(DataTypes.ShortType), milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
              case common.DataType.Int32 => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.createArrayType(DataTypes.IntegerType), milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
              case common.DataType.Int64 => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.createArrayType(DataTypes.LongType), milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
              case common.DataType.Float => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.createArrayType(DataTypes.FloatType), milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
              case common.DataType.Double => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.createArrayType(DataTypes.DoubleType), milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
              case common.DataType.String => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.createArrayType(DataTypes.StringType), milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
              case common.DataType.VarChar => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.createArrayType(DataTypes.StringType), milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
              case _ => throw new RuntimeException("Unsupported milvus array element data type: " + milvusFieldSchema.getElementType.toString() + " in field: " + milvusFieldSchema.getName)
            }
          }
          case common.DataType.BinaryVector => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.createArrayType(DataTypes.ByteType), milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
          case common.DataType.FloatVector => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.createArrayType(DataTypes.FloatType), milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
          case common.DataType.Float16Vector => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.createArrayType(DataTypes.ByteType), milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
          case common.DataType.BFloat16Vector => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.createArrayType(DataTypes.ByteType), milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
          case common.DataType.SparseFloatVector => sparkStructType = sparkStructType.add(milvusFieldSchema.getName, DataTypes.createMapType(DataTypes.LongType, DataTypes.FloatType), milvusFieldSchema.getIsNullable, milvusFieldSchema.getDescription);
          case _ => throw new RuntimeException("Unsupported milvus data type: " + milvusFieldSchema.getDataType.toString() + " in field: " + milvusFieldSchema.getName)
        }
      }
    }
    sparkStructType
  }

  private def milvusData2SparkData(milvusDesc: DescribeCollectionResp, milvusRowValues: Array[Object]): Array[Object] = {
    val fieldNames: Array[String] = sparkSchemas.fieldNames
    val values = ArrayBuffer[Object]()
    for (i <- 0 until fieldNames.size - 1) {
      milvusDesc.getCollectionSchema.getFieldSchemaList.get(i).getDataType match {
        case common.DataType.Bool => values += milvusRowValues(i).asInstanceOf[java.lang.Boolean]
        case common.DataType.Int8 => {
          val value = milvusRowValues(i).asInstanceOf[java.lang.Integer]
          if (value >= Byte.MinValue && value <= Byte.MaxValue) {
            values += value.toByte.asInstanceOf[java.lang.Byte]
          } else {
            throw new RuntimeException(s"Value $value out of Byte range")
          }
        }
        case common.DataType.Int16 => {
          val value = milvusRowValues(i).asInstanceOf[java.lang.Integer]
          if (value >= Short.MinValue && value <= Short.MaxValue) {
            values += value.toShort.asInstanceOf[java.lang.Short]
          } else {
            throw new RuntimeException(s"Value $value out of Short range")
          }
        }
        case common.DataType.Int32 => values += milvusRowValues(i).asInstanceOf[java.lang.Integer]
        case common.DataType.Int64 => values += milvusRowValues(i).asInstanceOf[java.lang.Long]
        case common.DataType.Float => values += milvusRowValues(i).asInstanceOf[java.lang.Float]
        case common.DataType.Double => values += milvusRowValues(i).asInstanceOf[java.lang.Double]
        case common.DataType.String => values += milvusRowValues(i).asInstanceOf[String]
        case common.DataType.VarChar => values += milvusRowValues(i).asInstanceOf[String]
        case common.DataType.JSON => values += milvusRowValues(i).asInstanceOf[JsonObject].toString
        case common.DataType.Array => {
          milvusDesc.getCollectionSchema.getFieldSchemaList.get(i).getElementType match {
            case common.DataType.Bool => {
              val dataArray = new ArrayBuffer[Boolean]()
              milvusRowValues(i).asInstanceOf[util.List[Boolean]].asScala.foreach(dataArray += _)
              values += dataArray.toArray
            }
            case common.DataType.Int8 => {
              val dataArray = new ArrayBuffer[Byte]()
              milvusRowValues(i).asInstanceOf[util.List[Byte]].asScala.foreach(dataArray += _)
              values += dataArray.toArray
            }
            case common.DataType.Int16 => {
              val dataArray = new ArrayBuffer[Short]()
              milvusRowValues(i).asInstanceOf[util.List[Short]].asScala.foreach(dataArray += _)
              values += dataArray.toArray
            }
            case common.DataType.Int32 => {
              val dataArray = new ArrayBuffer[Integer]()
              milvusRowValues(i).asInstanceOf[util.List[Integer]].asScala.foreach(dataArray += _)
              values += dataArray.toArray
            }
            case common.DataType.Int64 => {
              val dataArray = new ArrayBuffer[Long]()
              milvusRowValues(i).asInstanceOf[util.List[Long]].asScala.foreach(dataArray += _)
              values += dataArray.toArray
            }
            case common.DataType.Float => {
              val dataArray = new ArrayBuffer[Float]()
              milvusRowValues(i).asInstanceOf[util.List[Float]].asScala.foreach(dataArray += _)
              values += dataArray.toArray
            }
            case common.DataType.Double => {
              val dataArray = new ArrayBuffer[Double]()
              milvusRowValues(i).asInstanceOf[util.List[Double]].asScala.foreach(dataArray += _)
              values += dataArray.toArray
            }
            case common.DataType.String => {
              val dataArray = new ArrayBuffer[String]()
              milvusRowValues(i).asInstanceOf[util.List[String]].asScala.foreach(dataArray += _)
              values += dataArray.toArray
            }
            case common.DataType.VarChar => {
              val dataArray = new ArrayBuffer[String]()
              milvusRowValues(i).asInstanceOf[util.List[String]].asScala.foreach(dataArray += _)
              values += dataArray.toArray
            }
            case _ => throw new RuntimeException("Unsupported milvus array element data type: " + milvusDesc.getCollectionSchema.getFieldSchemaList.get(i).getElementType.toString() + " in field: " + fieldNames(i))
          }
        }
        case common.DataType.BinaryVector => {
          val data = milvusRowValues(i).asInstanceOf[ByteBuffer]
          data.rewind()
          val arr: Array[Byte] = data.array()
          values += arr
        }
        case common.DataType.FloatVector => {
          val dataList = new ArrayBuffer[Float]()
          milvusRowValues(i).asInstanceOf[util.List[Float]].asScala.foreach(dataList += _)
          values += dataList.toArray
        }
        case common.DataType.Float16Vector => {
          val data = milvusRowValues(i).asInstanceOf[ByteBuffer]
          data.rewind()
          val arr: Array[Byte] = data.array()
          values += arr
        }
        case common.DataType.BFloat16Vector => {
          val data = milvusRowValues(i).asInstanceOf[ByteBuffer]
          data.rewind()
          val arr: Array[Byte] = data.array()
          values += arr
        }
        case common.DataType.SparseFloatVector => {
          var map: SortedMap[Long, Float] = SortedMap.empty[Long, Float]
          milvusRowValues(i).asInstanceOf[util.TreeMap[Long, Float]].asScala.foreach { case (k, v) => map += k -> v }
          values += map
        }
        case _ => values += milvusRowValues(i)
      }
    }
    values.toArray
  }

  private def SparkData2milvusData(milvusDesc: DescribeCollectionResp, row: Row): JsonObject = {
    val milvusRow = new JsonObject()
    val gson = new Gson()

    val milvusFuncs = milvusDesc.getCollectionSchema.getFunctionList
    val funcOutPutFields = ListBuffer[String]()
    if (milvusFuncs != null && milvusFuncs.size() > 0) {
      milvusFuncs.asScala.foreach(func => {
        funcOutPutFields ++= func.getOutputFieldNames.asScala
      })
    }
    val funcOutputFieldsList = funcOutPutFields.toList

    for (i <- 0 until milvusDesc.getCollectionSchema.getFieldSchemaList.size) {
      val fieldSchema = milvusDesc.getCollectionSchema.getFieldSchemaList.get(i)
      if (!fieldSchema.getAutoID) {
        if (funcOutputFieldsList == null || (funcOutputFieldsList != null && !funcOutputFieldsList.contains(fieldSchema.getName))) {
          fieldSchema.getDataType match {
            case common.DataType.Bool => milvusRow.addProperty(fieldSchema.getName, row.getAs[Boolean](fieldSchema.getName))
            case common.DataType.Int8 => milvusRow.addProperty(fieldSchema.getName, row.getAs[Byte](fieldSchema.getName))
            case common.DataType.Int16 => milvusRow.addProperty(fieldSchema.getName, row.getAs[Short](fieldSchema.getName))
            case common.DataType.Int32 => milvusRow.addProperty(fieldSchema.getName, row.getAs[Integer](fieldSchema.getName))
            case common.DataType.Int64 => milvusRow.addProperty(fieldSchema.getName, row.getAs[Long](fieldSchema.getName))
            case common.DataType.Float => milvusRow.addProperty(fieldSchema.getName, row.getAs[Float](fieldSchema.getName))
            case common.DataType.Double => milvusRow.addProperty(fieldSchema.getName, row.getAs[Double](fieldSchema.getName))
            case common.DataType.String => milvusRow.addProperty(fieldSchema.getName, row.getAs[String](fieldSchema.getName))
            case common.DataType.VarChar => milvusRow.addProperty(fieldSchema.getName, row.getAs[String](fieldSchema.getName))
            case common.DataType.JSON => milvusRow.add(fieldSchema.getName, gson.fromJson(row.getAs[String](fieldSchema.getName), classOf[com.google.gson.JsonElement]))
            case common.DataType.Array => {
              fieldSchema.getElementType match {
                case common.DataType.Bool => {
                  val jsonArray = new JsonArray()
                  row.getList[Boolean](i).asScala.foreach(jsonArray.add(_))
                  milvusRow.add(fieldSchema.getName, jsonArray)
                }
                case common.DataType.Int8 => {
                  val jsonArray = new JsonArray()
                  row.getList[Byte](i).asScala.foreach(jsonArray.add(_))
                  milvusRow.add(fieldSchema.getName, jsonArray)
                }
                case common.DataType.Int16 => {
                  val jsonArray = new JsonArray()
                  row.getList[Short](i).asScala.foreach(jsonArray.add(_))
                  milvusRow.add(fieldSchema.getName, jsonArray)
                }
                case common.DataType.Int32 => {
                  val jsonArray = new JsonArray()
                  row.getList[Integer](i).asScala.foreach(jsonArray.add(_))
                  milvusRow.add(fieldSchema.getName, jsonArray)
                }
                case common.DataType.Int64 => {
                  val jsonArray = new JsonArray()
                  row.getList[Long](i).asScala.foreach(jsonArray.add(_))
                  milvusRow.add(fieldSchema.getName, jsonArray)
                }
                case common.DataType.Float => {
                  val jsonArray = new JsonArray()
                  row.getList[Float](i).asScala.foreach(jsonArray.add(_))
                  milvusRow.add(fieldSchema.getName, jsonArray)
                }
                case common.DataType.Double => {
                  val jsonArray = new JsonArray()
                  row.getList[Double](i).asScala.foreach(jsonArray.add(_))
                  milvusRow.add(fieldSchema.getName, jsonArray)
                }
                case common.DataType.String => {
                  val jsonArray = new JsonArray()
                  row.getList[String](i).asScala.foreach(jsonArray.add(_))
                  milvusRow.add(fieldSchema.getName, jsonArray)
                }
                case common.DataType.VarChar => {
                  val jsonArray = new JsonArray()
                  row.getList[String](i).asScala.foreach(jsonArray.add(_))
                  milvusRow.add(fieldSchema.getName, jsonArray)
                }
                case _ => throw new RuntimeException("Unsupported milvus array element data type: " + fieldSchema.getElementType.toString() + " in field: " + fieldSchema.getName)
              }
            }
            case common.DataType.BinaryVector => milvusRow.add(fieldSchema.getName, gson.toJsonTree(row.getList[Byte](i)))
            case common.DataType.FloatVector => milvusRow.add(fieldSchema.getName, gson.toJsonTree(row.getList[Float](i)))
            case common.DataType.Float16Vector => milvusRow.add(fieldSchema.getName, gson.toJsonTree(row.getList[Byte](i)))
            case common.DataType.BFloat16Vector => milvusRow.add(fieldSchema.getName, gson.toJsonTree(row.getList[Byte](i)))
            case common.DataType.SparseFloatVector => {
              val data = row.getMap[Long, Float](i)
              val sparse = new util.TreeMap[Long, Float]
              data.foreach { case (k, v) => sparse.put(k, v) }
              milvusRow.add(fieldSchema.getName, gson.toJsonTree(sparse))
            }
            case _ => throw new RuntimeException("Unsupported milvus data type: " + fieldSchema.getDataType.toString() + " in field: " + fieldSchema.getName)
          }
        }
      }
    }
    milvusRow
  }

  private def getValuesBySortedKey(
                                    map: util.Map[String, Object]
                                  ): Array[Object] = {
    val fieldNames = sparkSchemas.fieldNames
    val values = ArrayBuffer[Object]()
    for (i <- 0 until fieldNames.size - 1) {
      values += map.get(fieldNames(i))
    }
    values.toArray
  }

}

object MilvusRelation {
  private val Log = LoggerFactory.getLogger(this.getClass)
  private val LogTag = "+++++++++++++++++++++++++++++"
}
