package com.dolphindb.spark.partition

import com.dolphindb.spark.rdd.DolphinDBRDD
import com.dolphindb.spark.schema.{DolphinDBOptions, DolphinDBSchema}
import com.xxdb.DBConnection
import com.xxdb.data.{BasicAnyVector, Utils, Vector}
import org.apache.spark.Partition
import org.apache.spark.sql.sources._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class DolphinDBPartitioner(option: DolphinDBOptions) extends Serializable {

  def computGenericPartition(filters: Array[Filter]): Array[Partition]= {
    /**
      * Contains all Spark partition from DolphinDB
      */
    val partitions = new ArrayBuffer[DolphinDBPartition]()

    val conn = new DBConnection
    conn.connect(option.ip.get, option.port.get.toInt, option.user.get, option.password.get)
    
    val partiCols : Array[String] = DolphinDBSchema.getPartitionColumns(conn, option)
    val partiVals : Vector = DolphinDBSchema.getPartitionVals(conn,option)
    val addrs : mutable.HashMap[String, ArrayBuffer[Int]] = DolphinDBSchema.getAllDdataNodeAddr(conn, option)
    conn.close()

    /**
      * Screening filter conditions
      * Just need filter about partition column
      */
    val partiFilters = new ArrayBuffer[Filter]()

    val filterStr : Array[String]= filters.flatMap(DolphinDBRDD.compilerFilter(_))
    for (i <- 0 until(filterStr.length)) {
      partiCols.foreach(p => if(filterStr(i).toLowerCase.contains(p.toLowerCase)) {
        partiFilters += filters(i)
      })
    }

    /**
      * Must judge partiFilters.length == 0.
      *  if partiFilters.length==0 ,need add all DolphinDB partition into spark partition.
      *  if not , need select the appropriate partitions.
      **/
      /** There are multiple partitions in DolphinDB table */
    if (partiVals.isInstanceOf[BasicAnyVector] && partiCols.length > 1) {
      var partiValArr = new ArrayBuffer[ArrayBuffer[String]]()
      val partiVector = partiVals.asInstanceOf[BasicAnyVector]

      /**    There are not partition field in User-defined SQL */
      if (partiFilters.length == 0) {
        for (i <- 0 until(partiVector.rows())) {
          val tmpPartiVal = new ArrayBuffer[ArrayBuffer[String]]()
          val vector = partiVector.getEntity(i).asInstanceOf[Vector]
          for (j <- 0 until(vector.rows())) {
            /**  i = 0 means the first level partition     */
            if (i == 0) {
              partiValArr += ArrayBuffer[String](vector.get(j).toString)
            } else {
              for (pv <- 0 until (partiValArr.size)) {
                val tmpBuf = new ArrayBuffer[String]()
                partiValArr(pv).copyToBuffer(tmpBuf)
                tmpBuf += vector.get(j).toString
                tmpPartiVal += tmpBuf
              }
              partiValArr = tmpPartiVal
            }
          }
        }
      }
      /** There are partition fields in User-defined SQL   */
      else {

      }

    }
    /**  There is only one partition in DolphinDB table. */
    else {
      /**  There are not partition field in User-defined SQL */
      if (partiFilters.length == 0){
        for (pi <- 0 until(partiVals.rows())) {
          partitions += new DolphinDBPartition(pi, addrs, partiCols, Array(partiVals.get(pi).toString))
        }
      }
      /**  There are partition fields in User-defined SQL */
      else {
        val partiValArr = new ArrayBuffer[ArrayBuffer[String]]()
        for (pi <- 0 until(partiVals.rows())) {
          if (getDolphinDBPartitionBySingleFilter(partiCols(0), partiVals.get(pi).toString, partiFilters.toArray)) {
            partiValArr += ArrayBuffer(partiVals.get(pi).toString)
          }
        }
      }
    }

    /**
      * 此处要处理filter 与  dolphindb 中的分区字段匹配， 不是所有的都要处理，
      * 如果用户的查询条件就是有分区字段，就是不用去扫描所有的分区字段。
      */

    partitions.toArray
  }

  private def getDolphinDBPartitionBySingleFilter (
          partCol :String, partiVal : String,
          partFilter: Array[Filter]) : Boolean = {

    /**   Get the partiton column type in dolphindb  */
     val partType = DolphinDBRDD.originNameToType.get(partCol).get.toUpperCase

     if(partType.equals("STRING") || partType.equals("SYMBOL")) {
       var flagPart = true
       partFilter.foreach(f => f match {
           case EqualTo(attr, value) => {
             if (!(attr.equalsIgnoreCase(partCol) &&
               partiVal.toString.equals(value.toString))) {
               flagPart = false
             }
           }
           case LessThan(attr, value) => {
             if (!(attr.equalsIgnoreCase(partCol) &&
               partiVal.toString < value.toString)) {
               flagPart = false
             }
           }
           case GreaterThan(attr, value) => {
             if (!(attr.equalsIgnoreCase(partCol) &&
               partiVal.toString > value.toString)) {
               flagPart = false
             }
           }
           case LessThanOrEqual(attr, value) => {
             if (!(attr.equalsIgnoreCase(partCol) &&
               partiVal.toString <= value.toString)) {
               flagPart = false
             }
           }
           case GreaterThanOrEqual(attr, value) => {
             if (!(attr.equalsIgnoreCase(partCol) &&
               partiVal.toString >= value.toString)) {
               flagPart = false
             }
           }
           case StringStartsWith(attr, value) => {
             val str = value.substring(0, attr.indexOf("%"))
             if (!attr.equalsIgnoreCase(partCol) &&
               partiVal.toString.startsWith(str)) {
               flagPart = false
             }
           }
           case StringEndsWith(attr, value) => {
             val str = value.substring(attr.indexOf("%") + 1)
             if (!attr.equalsIgnoreCase(partCol) &&
               partiVal.toString.endsWith(str)) {
               flagPart = false
             }
           }
           case StringContains(attr, value) => {
             val str = value.substring(1, value.lastIndexOf("%") + 1)
             if (!attr.equalsIgnoreCase(partCol) &&
               partiVal.toString.contains(str)) {
               flagPart = false
             }
           }
           case In(attr, value) => {
             var vin = false
             value.foreach(v => if (v.toString.equals(partiVal)) vin = true)
             if (!attr.equalsIgnoreCase(partCol) && vin) {
               flagPart = false
             }
           }
           case Not(f) => {
              if (getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
                flagPart = false
              }
           }
           case Or(f1, f2) => {
             if (!getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
               getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
               flagPart = false
             }
           }
           case And(f1, f2) => {
             if (!getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
               getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
               flagPart = false
             }
           }
         })
       flagPart
     } else if (partType.equals("INT") || partType.equals("LONG")
          || partType.equals("SHORT")){
       var flagPart = true
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           if (!(attr.equalsIgnoreCase(partCol) &&
             partiVal.toLong == value.toString.toLong)) {
             flagPart = false
           }
         }
         case LessThan(attr, value) => {
           if (!(attr.equalsIgnoreCase(partCol) &&
             partiVal.toLong < value.toString.toLong)) {
             flagPart = false
           }
         }
         case GreaterThan(attr, value) => {
           if (!(attr.equalsIgnoreCase(partCol) &&
             partiVal.toLong > value.toString.toLong)) {
             flagPart = false
           }
         }
         case LessThanOrEqual(attr, value) => {
           if (!(attr.equalsIgnoreCase(partCol) &&
             partiVal.toLong <= value.toString.toLong)) {
             flagPart = false
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           if (!(attr.equalsIgnoreCase(partCol) &&
             partiVal.toLong >= value.toString.toLong)) {
             flagPart = false
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach(v => if (v.toString.toLong == partiVal.toLong) vin = true)
           if (!attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = false
           }
         }
         case Not(f) => {
           if (getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = false
           }
         }
         case Or(f1, f2) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = false
           }
         }
         case And(f1, f2) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = false
           }
         }
       })
       flagPart
     } else if (partType.equals("CHAR")) {
       var flagPart = true
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           if (!(attr.equalsIgnoreCase(partCol) &&
             partiVal.charAt(0).toByte == value.toString.toByte)) {
             flagPart = false
           }
         }
         case LessThan(attr, value) => {
           if (!(attr.equalsIgnoreCase(partCol) &&
             partiVal.charAt(0).toByte < value.toString.toByte)) {
             flagPart = false
           }
         }
         case GreaterThan(attr, value) => {
           if (!(attr.equalsIgnoreCase(partCol) &&
             partiVal.charAt(0).toByte > value.toString.toByte)) {
             flagPart = false
           }
         }
         case LessThanOrEqual(attr, value) => {
           if (!(attr.equalsIgnoreCase(partCol) &&
             partiVal.charAt(0).toByte <= value.toString.toByte)) {
             flagPart = false
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           if (!(attr.equalsIgnoreCase(partCol) &&
             partiVal.charAt(0).toByte >= value.toString.toByte)) {
             flagPart = false
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach(v => if (v.toString.charAt(0).toByte == partiVal.toByte) vin = true)
           if (!attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = false
           }
         }
         case Not(f) => {
           if (getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = false
           }
         }
         case Or(f1, f2) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = false
           }
         }
         case And(f1, f2) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = false
           }
         }
       })
       flagPart
     } else if (partType.equals("FLOAT") || partType.equals("DOUBLE")) {
       var flagPart = true
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           if (!(attr.equalsIgnoreCase(partCol) &&
             partiVal.toDouble == value.toString.toDouble)) {
             flagPart = false
           }
         }
         case LessThan(attr, value) => {
           if (!(attr.equalsIgnoreCase(partCol) &&
             partiVal.toDouble < value.toString.toDouble)) {
             flagPart = false
           }
         }
         case GreaterThan(attr, value) => {
           if (!(attr.equalsIgnoreCase(partCol) &&
             partiVal.toDouble > value.toString.toDouble)) {
             flagPart = false
           }
         }
         case LessThanOrEqual(attr, value) => {
           if (!(attr.equalsIgnoreCase(partCol) &&
             partiVal.toDouble <= value.toString.toDouble)) {
             flagPart = false
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           if (!(attr.equalsIgnoreCase(partCol) &&
             partiVal.toDouble >= value.toString.toDouble)) {
             flagPart = false
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach(v => if (v.toString.toDouble == partiVal.toDouble) vin = true)
           if (!attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = false
           }
         }
         case Not(f) => {
           if (getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = false
           }
         }
         case Or(f1, f2) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = false
           }
         }
         case And(f1, f2) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = false
           }
         }
       })
       flagPart
     } else if (partType.equals("MONTH")) {
       var flagPart = true
       val partMon = partiVal.replace("M", "").split(".")
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           var userMonstr :String = null
           if (value.toString.contains("M")){
             userMonstr = value.toString.replace("M", "")
           }
           val userMon = userMonstr.split("-")
           if (!(attr.equalsIgnoreCase(partCol) &&
             Utils.countMonths(partMon(0).toInt, partMon(1).toInt) ==
               Utils.countMonths(userMon(0).toInt, userMon(1).toInt))) {
             flagPart = false
           }
         }
         case LessThan(attr, value) => {
           var userMonstr :String = null
           if (value.toString.contains("M")){
             userMonstr = value.toString.replace("M", "")
           }
           val userMon = userMonstr.split("-")
           if (!(attr.equalsIgnoreCase(partCol) &&
             Utils.countMonths(partMon(0).toInt, partMon(1).toInt) <
               Utils.countMonths(userMon(0).toInt, userMon(1).toInt))) {
             flagPart = false
           }
         }
         case GreaterThan(attr, value) => {
           var userMonstr :String = null
           if (value.toString.contains("M")){
             userMonstr = value.toString.replace("M", "")
           }
           val userMon = userMonstr.split("-")
           if (!(attr.equalsIgnoreCase(partCol) &&
             Utils.countMonths(partMon(0).toInt, partMon(1).toInt) >
               Utils.countMonths(userMon(0).toInt, userMon(1).toInt))) {
             flagPart = false
           }
         }
         case LessThanOrEqual(attr, value) => {
           var userMonstr :String = null
           if (value.toString.contains("M")){
             userMonstr = value.toString.replace("M", "")
           }
           val userMon = userMonstr.split("-")
           if (!(attr.equalsIgnoreCase(partCol) &&
             Utils.countMonths(partMon(0).toInt, partMon(1).toInt) <=
               Utils.countMonths(userMon(0).toInt, userMon(1).toInt))) {
             flagPart = false
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           var userMonstr :String = null
           if (value.toString.contains("M")){
             userMonstr = value.toString.replace("M", "")
           }
           val userMon = userMonstr.split("-")
           if (!(attr.equalsIgnoreCase(partCol) &&
             Utils.countMonths(partMon(0).toInt, partMon(1).toInt) >=
               Utils.countMonths(userMon(0).toInt, userMon(1).toInt))) {
             flagPart = false
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach { v =>
             var mon: String = null
             if (v.toString.contains("M")) {
               mon = v.toString.replace("M", "")
             }
             if (mon.replace("-", ".").contains(
                    partiVal.replace("M", ""))) {
               vin = true
             }
           }
           if (!attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = false
           }
         }
         case Not(f) => {
           if (getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = false
           }
         }
         case Or(f1, f2) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = false
           }
         }
         case And(f1, f2) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = false
           }
         }
       })
       flagPart
     } else if (partType.equals("BOOL")) {

       false
     } else if (partType.equals("TIME")) {
       false
     } else if (partType.equals("MINUTE")) {
       false
     } else if (partType.equals("SECOND")) {
       false
     } else if (partType.equals("DATETIME")) {
       false
     } else if (partType.equals("TIMESTAMP")) {
       false
     } else if (partType.equals("NANOTIME")) {
       false
     } else if (partType.equals("NANOTIMESTAMP")) {
       false
     } else if (partType.equals("DATE")) {
       false
     } else {
       false
     }
  }









}
