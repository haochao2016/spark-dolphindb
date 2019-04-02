package com.dolphindb.spark.schema

import java.util

import com.xxdb.data._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._

object DolphinDBSchema extends Logging{

  /**
    * Convert DolphinDB DataType to Spark DataType
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