package com.dolphindb.spark.rdd

import java.sql.{Date, Timestamp}

import com.dolphindb.spark.schema.DolphinDBOptions
import com.xxdb.data.{BasicTable}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

class DolphinDBRDDIterator(
       taskContext : TaskContext,
       partition : Partition,
       options : DolphinDBOptions,
       schema : StructType,
       dolphinDBTable : BasicTable,
       queryLimit : Long= -1
    ) extends Iterator[Row]
  // InternalRow can not cast to Row, So Row .
  // InternalRow cast to Row except DolphinDBRelation override needConversion = false
      with Logging{

  override val size  = dolphinDBTable.rows()  // dolphinDBTable length
  private var cursor = 0   // index of next element to return
 // private var lastRet: Long = -1
  private val columnSize = schema.length

  override def hasNext: Boolean =  cursor != size

  override def next(): Row = {
    val i = cursor
    if (i >= size) {
      throw new NoSuchElementException("End of table")
    }
    val buffer = new ArrayBuffer[Any]()
    cursor += 1
    for (j <- 0 until(columnSize)) {
      val scalarVal = dolphinDBTable.getColumn(schema(j).name).get(i).toString
      val scalarType = schema(j).dataType
      val originType = DolphinDBRDD.originNameToType.get(schema(j).name).get
      buffer += applyTypeInData(scalarType, scalarVal, originType)
    }
     Row.fromSeq(buffer)
  }

  /**
    *  Converting data to corresponding types
    *  Convert DolphinDB dataType to Spark dataType
    * @param fieldType  data type in spark
    * @param fieldVal  data value
    * @param fieldOriginType  data type in DolphinDB
    * @return
    */
  def applyTypeInData(fieldType : DataType= StringType, fieldVal : String, fieldOriginType: String) : Any = fieldType match {

    case DateType => {
      fieldOriginType match {
        case "DATE" => Date.valueOf(fieldVal.replace(".", "-"))
        case "MONTH" =>
          Date.valueOf(fieldVal.replace("M", ".01")
            .replace(".", "-"))
      }
    }
    case TimestampType => {
      fieldOriginType match {
        case "TIME" =>
          Timestamp.valueOf("1970-01-01 " + fieldVal.toString)
        case "MINUTE" =>
          Timestamp.valueOf("1970-01-01 " + fieldVal.replace("m", ":01"))
        case "SECOND" =>
          Timestamp.valueOf("1970-01-01 " + fieldVal.toString)
        case "DATETIME" =>
          if (fieldVal.contains("T")) Timestamp.valueOf(fieldVal.replace("T", " "))
          else Timestamp.valueOf(fieldVal.toString)
        case "TIMESTAMP" =>
          if (fieldVal.contains("T")) Timestamp.valueOf(fieldVal.replace("T", " "))
          else Timestamp.valueOf(fieldVal.toString)
        case "NANOTIME" =>
          Timestamp.valueOf("1970-01-01 " + fieldVal.toString)
        case "NANOTIMESTAMP" =>
          if (fieldVal.contains("T")) Timestamp.valueOf(fieldVal.replace("T", " "))
          else Timestamp.valueOf(fieldVal.toString)
        case _ => fieldVal
      }
    }
    case StringType => fieldVal.toString
    case IntegerType => fieldVal.toInt
    case TimestampType => Timestamp.valueOf(fieldVal.replace("T" ," "))
    case NullType => null
    case BooleanType => if (fieldVal.equals("0") || fieldVal.toLowerCase().equals("false")) false else true
    case DoubleType => fieldVal.toDouble
    case FloatType => fieldVal.toFloat
    case LongType => fieldVal.toLong
    case ShortType => fieldVal.toShort
    case ByteType => fieldVal.charAt(0).toByte
    case _ => fieldVal
  }


}
