package com.dolphindb.spark.schema

import java.util

import com.xxdb.DBConnection
import com.xxdb.data._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DolphinDBSchema extends Logging{

  /**
    * Get All DolphinDB DataNode Address
    * @param conn
    * @param option
    */
  def getAllDdataNodeAddr(conn : DBConnection, option: DolphinDBOptions) = {
    val vector = conn.run(s"ctl = getControllerAlias();" +
            s"  exec site from rpc(ctl,getClusterPerf) where mode = 0").asInstanceOf[BasicStringVector]
    val addrBuffer = new mutable.HashMap[String, ArrayBuffer[Int]]()
    for (i <- 0 until(vector.rows())) {
      val addr = vector.get(i).toString.split(":")
      val ports = addrBuffer.getOrElse(addr(0), new ArrayBuffer[Int]())
      ports += addr(1).toInt
      addrBuffer.put(addr(0), ports)
//      val ports = if (addrBuffer.contains(addr(0))) {addrBuffer.get(addr(0)).asInstanceOf[ArrayBuffer[Int]]} else {
//        val portBuf = new ArrayBuffer[Int]()
//        addrBuffer += (addr(0) -> portBuf)
//        portBuf
//      }
//      ports += addr(1).toInt
    }
    addrBuffer
  }

  /**
    * Get DolphinDB Partition Values
    *
    * @param conn
    * @param option
    * @return
    */
  def getPartitionVals(conn: DBConnection, option: DolphinDBOptions): Vector = {
    val table = option.table
    val dbPath = option.dbPath
    val vector = conn.run(s"schema($table).partitionSchema").asInstanceOf[Vector]
    vector
  }

  /**
    * Get partition columns from  the DolphinDB Partition table.
    *
    * @param option DolphinDBOptions
    * @return
    */
  def getPartitionColumns(conn : DBConnection, option: DolphinDBOptions) : Array[String] = {
    val table = option.table
    val dbPath = option.dbPath
    val partiCols = conn.run(s"${table}=database('${dbPath}').loadTable('${table}'); schema(${table}).partitionColumnName")
    if (partiCols.isInstanceOf[com.xxdb.data.Void]) {
      return Array[String]()
    }
    if (partiCols.isInstanceOf[BasicStringVector]) {
      val vectorCol = partiCols.asInstanceOf[BasicStringVector]
      val vectorBuf = new ArrayBuffer[String]()
      for (i <- 0 until(vectorCol.rows())) {
        vectorBuf += vectorCol.getString(i)
      }
      vectorBuf.toArray
    } else {
      Array(partiCols.asInstanceOf[BasicString].getString)
    }

  }


  /**
    * Convert DolphinDB DataType to Spark DataType
    *
    * @param originName dataName
    * @param originType dataType in DolphinDB
    * @return
    */
  private[spark] def convertToStructField (originName : String ,
                                    originType : String) : StructField =  originType match {

    case "SYMBOL" => StructField(originName, StringType)
    case "STRING" => StructField(originName, StringType)

    case "DATE" => StructField(originName, DateType)
    case "MONTH" => StructField(originName, DateType)
    case "TIME" => StructField(originName, TimestampType)
    case "MINUTE" => StructField(originName, TimestampType)
    case "SECOND" => StructField(originName, TimestampType)
    case "DATETIME" => StructField(originName, TimestampType)
    case "TIMESTAMP" => StructField(originName, TimestampType)
    case "NANOTIME" => StructField(originName, TimestampType)
    case "NANOTIMESTAMP" => StructField(originName, TimestampType)

    case "VOID" => StructField(originName, NullType)
    case "BOOL" => StructField(originName, BooleanType)
    case "DOUBLE" => StructField(originName, DoubleType)
    case "FLOAT" => StructField(originName, FloatType)
    case "LONG" => StructField(originName, LongType)
    case "INT" => StructField(originName, IntegerType)
    case "SHORT" => StructField(originName, ShortType)
    case "CHAR" => StructField(originName, ByteType)
    case _ => StructField(originName, StringType)
  }

  /**
    * Convert Spark DataType to DolphinDB DataType
    * @param value dataName
    * @param dolphinDBType dataType in DolphinDB
    * @return
    */
  private[spark] def convertStructFieldToType (
         value : String,
         dolphinDBType : String) : Any = {
    val vectors = new util.ArrayList[Vector]()
    dolphinDBType match {

      case "SYMBOL" => vectors.add(new BasicStringVector(Array[String](if (value == null) "" else value)))
      case "DATE" => {
        if (value == null) {
          vectors.add(new BasicDateVector(Array[Int](0, 0, 0)))
        }
        val dates = value.split("-")
        vectors.add(new BasicDateVector(Array[Int](Utils.countDays(dates(0).toInt, dates(1).toInt, dates(2).toInt))))
      }
      case "SECOND" => {
        if (value == null) {
          vectors.add(new BasicSecondVector(Array[Int](0, 0, 0)))
        }
        val times = value.split(":")
        vectors.add(new BasicSecondVector(Array[Int](Utils.countSeconds(times(0).toInt, times(1).toInt, times(2).toInt))))
      }
      case "DOUBLE" => {
        if (value == null) {
          vectors.add(new BasicDoubleVector(Array[Double](0)))
        }
        vectors.add(new BasicDoubleVector(Array[Double](value.toDouble)))
      }
      case "INT" => {
        if (value == null) {
          vectors.add(new BasicIntVector(Array[Int](0)))
        }
        vectors.add(new BasicIntVector(Array[Int](value.toInt)))
      }

      case "CHAR" => {
        if (value == null) {
          vectors.add(new BasicByteVector(Array[Byte](0.toByte)))
        }
        vectors.add(new BasicByteVector(Array[Byte](value.charAt(0).toByte)))
      }
    }
  }




}