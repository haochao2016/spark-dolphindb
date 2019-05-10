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
        case "DATE" => if (fieldVal.equals("")) null else Date.valueOf(fieldVal.replace(".", "-"))
        case "MONTH" =>
          if (fieldVal.equals("")) null else
            Date.valueOf(fieldVal.replace("M", ".01")
            .replace(".", "-"))
      }
    }
    case TimestampType => {
      fieldOriginType match {
        case "TIME" =>
          if (fieldVal.equals("")) null else Timestamp.valueOf("1970-01-01 " + fieldVal.toString)
        case "MINUTE" =>
          if (fieldVal.equals("")) null else Timestamp.valueOf("1970-01-01 " + fieldVal.replace("m", ":01"))
        case "SECOND" =>
          if (fieldVal.equals("")) null else Timestamp.valueOf("1970-01-01 " + fieldVal.toString)
        case "DATETIME" =>
          if (fieldVal.equals("")) return null
          var fieldDTVal : String = null
          if (fieldVal.contains("T")) {
            fieldDTVal = fieldVal.split("T")(0).replace(".", "-") +
                " " + fieldVal.split("T")(1)
          } else {
            fieldDTVal = fieldVal.split(" ")(0).replace(".", "-") +
              " " + fieldVal.split(" ")(1)
          }
          Timestamp.valueOf(fieldDTVal)
        case "TIMESTAMP" =>
          if (fieldVal.equals("")) return null
          var fieldTMVal : String = null
          if (fieldVal.contains("T")) {
            fieldTMVal = fieldVal.split("T")(0).replace(".", "-") +
              " " + fieldVal.split("T")(1)
          } else {
            fieldTMVal = fieldVal.split(" ")(0).replace(".", "-") +
              " " + fieldVal.split(" ")(1)
          }
          Timestamp.valueOf(fieldTMVal)
        case "NANOTIME" =>
          if(fieldVal.equals("")) null else Timestamp.valueOf("1970-01-01 " + fieldVal.toString)
        case "NANOTIMESTAMP" =>
          var fieldNTMVal : String = null
          if(fieldVal.equals("")) {null
          } else {
            if (fieldVal.contains("T")) {
              fieldNTMVal = fieldVal.split("T")(0).replace(".", "-") +
                " " + fieldVal.split("T")(1)
            } else {
              fieldNTMVal = fieldVal.split(" ")(0).replace(".", "-") +
                " " + fieldVal.split(" ")(1)
            }
            Timestamp.valueOf(fieldNTMVal)
          }
        case _ => if(fieldVal.equals("")) null else fieldVal
      }
    }
    case StringType => if(fieldVal.equals("")) null else fieldVal.toString
    case IntegerType => if(fieldVal.equals("")) null else fieldVal.toInt
    case NullType => null
    case BooleanType => if(fieldVal.equals("")) null  else {if (fieldVal.equals("0") || fieldVal.toLowerCase().equals("false")) false else true}
    case DoubleType => if(fieldVal.equals("")) null else fieldVal.toDouble
    case FloatType => if(fieldVal.equals("")) null else fieldVal.toFloat
    case LongType => if(fieldVal.equals("")) null else fieldVal.toLong
    case ShortType => if(fieldVal.equals("")) null else  fieldVal.toShort
    case ByteType => if(fieldVal.equals("")) null else fieldVal.charAt(0).toByte
    case _ => if(fieldVal.equals("")) null else fieldVal
  }


}
