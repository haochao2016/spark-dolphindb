package com.dolphindb.spark.writer

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util

import com.dolphindb.spark.DolphinDBUtils
import com.dolphindb.spark.schema.DolphinDBOptions
import com.xxdb.data._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer

class DolphinDBWriter(options: DolphinDBOptions) extends Logging{

  private val dbPath = options.dbPath
  private val table = options.table
  val conn = DolphinDBUtils.createDolphinDBConn(options)

  /**
    *  Save Spark DataFrame into DolphinDB
    * @param it
    * @param name2Type  dolphinDB table schema ,
    *  example:
    * {{{
    *   name -> SYMBOL,
    *   age -> INT
    *   price -> DOUBLE
    * }}}
    */
  def save( it: Iterator[Row], name2Type : Array[(String, String)]): Unit = {

    /**
      * DolphinDB table column names
      */
    val dolphindbNames = new util.ArrayList[String]()
    /**
      * contain all data that insert into DolphinDB
      */
    val dataBuffer = new ArrayBuffer[Any]()

    for (nt <- name2Type) {
      dolphindbNames.add(nt._1)

      nt._2.toUpperCase  match {
        case "SYMBOL" =>
          dataBuffer += new ArrayBuffer[String]()
        case "STRING" =>
          dataBuffer += new ArrayBuffer[String]()
        case "DATE" =>
          dataBuffer += new ArrayBuffer[Int]()
        case "MONTH" =>
          dataBuffer += new ArrayBuffer[Int]()
        case "TIME" =>
          dataBuffer += new ArrayBuffer[Int]()
        case "MINUTE" =>
          dataBuffer += new ArrayBuffer[Int]()
        case "SECOND" =>
          dataBuffer += new ArrayBuffer[Int]()
        case "DATETIME" =>
          dataBuffer += new ArrayBuffer[Int]()
        case "TIMESTAMP" =>
          dataBuffer += new ArrayBuffer[Long]()
        case "NANOTIME" =>
          dataBuffer += new ArrayBuffer[Long]()
        case "NANOTIMESTAMP" =>
          dataBuffer += new ArrayBuffer[Long]()
        case "VOID" =>
          dataBuffer += new ArrayBuffer[Null]()
        case "BOOL" =>
          dataBuffer += new ArrayBuffer[Byte]()
        case "DOUBLE" =>
          dataBuffer += new ArrayBuffer[Double]()
        case "FLOAT" =>
          dataBuffer += new ArrayBuffer[Float]()
        case "LONG" =>
          dataBuffer += new ArrayBuffer[Long]()
        case "INT" =>
          dataBuffer += new ArrayBuffer[Int]()
        case "SHORT" =>
          dataBuffer += new ArrayBuffer[Short]()
        case "CHAR" =>
          dataBuffer += new ArrayBuffer[Byte]()
        case _ =>
          dataBuffer += new ArrayBuffer[String]()
      }
    }

    while (it.hasNext) {
       val row = it.next()

       for (i <- 0 until(row.length)) {
         name2Type(i)._2.toUpperCase match {
           case "SYMBOL" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[String]] += row(i).toString
           case "STRING" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[String]] += row(i).toString
           case "DATE" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[Int]] +=
              Utils.countDays(LocalDate.parse(row(i).toString))
           case "MONTH" =>
             val monthStr = row(i).toString.split("-")
               dataBuffer(i).asInstanceOf[ArrayBuffer[Int]] +=
                 Utils.countMonths(monthStr(0).toInt, monthStr(1).toInt)
           case "TIME" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[Int]] +=
              Utils.countMilliseconds(LocalTime.parse(row(i).toString.split(" ")(1)))
           case "MINUTE" =>
             val minute = row(i).toString.split(" ")(1)
             dataBuffer(i).asInstanceOf[ArrayBuffer[Int]] +=
                Utils.countMinutes(minute.split(":")(0).toInt,
                  minute.split(":")(1).toInt)
           case "SECOND" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[Int]] +=
                Utils.countSeconds(LocalTime.parse(row(i).toString.split(" ")(1)))
           case "DATETIME" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[Int]] +=
                Utils.countSeconds(LocalDateTime.parse(row(i).toString))
           case "TIMESTAMP" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[Long]] +=
                Utils.countMilliseconds(LocalDateTime.parse(row(i).toString))
           case "NANOTIME" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[Long]] +=
                Utils.countNanoseconds(LocalTime.parse(row(i).toString.split(" ")(1)))
           case "NANOTIMESTAMP" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[Long]] +=
                Utils.countNanoseconds(LocalDateTime.parse(row(i).toString))
           case "VOID" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[Null]] += null
           case "BOOL" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[Byte]] +=
              (if (java.lang.Boolean.parseBoolean(row(i).toString)) 1.toByte else 0.toByte)
           case "DOUBLE" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[Double]] +=
                row(i).toString.toDouble
           case "FLOAT" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[Float]] +=
               row(i).toString.toFloat
           case "LONG" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[Long]] +=
               row(i).toString.toLong
           case "INT" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[Int]] +=
               row(i).toString.toInt
           case "SHORT" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[Short]] +=
               row(i).toString.toShort
           case "CHAR" =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[Byte]] +=
               row(i).toString.toByte
           case _ =>
             dataBuffer(i).asInstanceOf[ArrayBuffer[String]] += row(i).toString
         }
      }

      if (dataBuffer(0).asInstanceOf[ArrayBuffer[Any]].size > DolphinDBUtils.DolphinDB_PER_NUM) {
        saveIntoDolphinDB(dataBuffer, name2Type, dolphindbNames)
      }
    }
    saveIntoDolphinDB(dataBuffer, name2Type, dolphindbNames)
    conn.close()
  }


  private def saveIntoDolphinDB(dataBuffer: ArrayBuffer[Any],
      name2Type : Array[(String, String)], dolphindbNames : util.ArrayList[String]): Unit = {

    val vectors = new util.ArrayList[Vector]()
    for (i <- 0 until(name2Type.length)) {
      name2Type(i)._2.toUpperCase match {
        case "SYMBOL" =>
          vectors.add(new BasicStringVector(dataBuffer(i).asInstanceOf[ArrayBuffer[String]].toArray))
        case "STRING" =>
          vectors.add(new BasicStringVector(dataBuffer(i).asInstanceOf[ArrayBuffer[String]].toArray))
        case "DATE" =>
          vectors.add(new BasicDateVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Int]].toArray))
        case "MONTH" =>
          vectors.add(new BasicMonthVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Int]].toArray))
        case "TIME" =>
          vectors.add(new BasicTimeVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Int]].toArray))
        case "MINUTE" =>
          vectors.add(new BasicMinuteVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Int]].toArray))
        case "SECOND" =>
          vectors.add(new BasicSecondVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Int]].toArray))
        case "DATETIME" =>
          vectors.add(new BasicDateTimeVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Int]].toArray))
        case "TIMESTAMP" =>
          vectors.add(new BasicTimestampVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Long]].toArray))
        case "NANOTIME" =>
          vectors.add(new BasicNanoTimeVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Long]].toArray))
        case "NANOTIMESTAMP" =>
          vectors.add(new BasicNanoTimestampVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Long]].toArray))
        case "VOID" =>
        //              vectors.add(new BasicLongVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Null]].toArray))
        case "BOOL" =>
          vectors.add(new BasicBooleanVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Byte]].toArray))
        case "DOUBLE" =>
          vectors.add(new BasicDoubleVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Double]].toArray))
        case "FLOAT" =>
          vectors.add(new BasicFloatVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Float]].toArray))
        case "LONG" =>
          vectors.add(new BasicLongVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Long]].toArray))
        case "INT" =>
          vectors.add(new BasicIntVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Int]].toArray))
        case "SHORT" =>
          vectors.add(new BasicShortVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Short]].toArray))
        case "CHAR" =>
          vectors.add(new BasicByteVector(dataBuffer(i).asInstanceOf[ArrayBuffer[Byte]].toArray))
        case _ =>
          vectors.add(new BasicStringVector(dataBuffer(i).asInstanceOf[ArrayBuffer[String]].toArray))
      }
    }
    val entities = new util.ArrayList[Entity]()
    entities.add(new BasicTable(dolphindbNames, vectors))
    conn.run(s"append!{loadTable('${dbPath}', '${table}')}", entities)

  }



}
