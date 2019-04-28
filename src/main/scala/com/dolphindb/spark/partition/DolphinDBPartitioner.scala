package com.dolphindb.spark.partition

import java.time.{LocalDate, LocalDateTime, LocalTime}

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
    if (partiCols.length == 0) {
      partitions += new DolphinDBPartition(0, null, null,null)
      return partitions.toArray
    }
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
    var partiValArr = new ArrayBuffer[ArrayBuffer[String]]()  // Contains all partition value in Spark
      /** There are multiple partitions in DolphinDB table */
    if (partiVals.isInstanceOf[BasicAnyVector] && partiCols.length > 1) {
      val partiVector = partiVals.asInstanceOf[BasicAnyVector]

      for (i <- 0 until(partiVector.rows())) {
        val tmpPartiVal = new ArrayBuffer[ArrayBuffer[String]]()
        val vector = partiVector.getEntity(i).asInstanceOf[Vector]
        for (j <- 0 until(vector.rows())) {
          /**  i = 0 means the first level partition in DolphinDB   */
          if (i == 0) {
            /** There are partition filter in User-defined SQL   */
            if (partiFilters.length != 0 && getDolphinDBPartitionBySingleFilter(partiCols(i),
                  vector.get(j).toString, partiFilters.toArray)) {
              partiValArr += ArrayBuffer[String](vector.get(j).toString)
            } else {
            /**    There are not partition filter in User-defined SQL */
              partiValArr += ArrayBuffer[String](vector.get(j).toString)
            }
          } else {
            /**  means not the first level partition in DolphinDB */
            var addPartFlag = true
            /** There are partition filter in User-defined SQL   */
            if (!(partiFilters.length != 0 && getDolphinDBPartitionBySingleFilter(partiCols(i),
              vector.get(j).toString, partiFilters.toArray))){
              addPartFlag = false
            }
            if (addPartFlag) {
              for (pv <- 0 until (partiValArr.size)) {
                val tmpBuf = new ArrayBuffer[String]()
                partiValArr(pv).copyToBuffer(tmpBuf)
                tmpBuf += vector.get(j).toString
                tmpPartiVal += tmpBuf
              }
            }
          }
        }
        partiValArr = tmpPartiVal
        tmpPartiVal.clear()
      }
    } else {
      /**  There is only one partition in DolphinDB table. */
      /////////////////////////////////////////////////////////
      /**  There are not partition filter in User-defined SQL */
        for (pi <- 0 until(partiVals.rows())) {
          var addPartFlag = true
          /**  There are partition filter in User-defined SQL, So filter partition  */
          if (!(partiFilters.length != 0 && getDolphinDBPartitionBySingleFilter(partiCols(0),
            partiVals.get(pi).toString, partiFilters.toArray))) {
            addPartFlag = false
          }
          if (addPartFlag) {
            partiValArr += ArrayBuffer(partiVals.get(pi).getString)
          }
        }
    }

    /**   Create spark partitions based on filtered partition values     */
    for (pi <- 0 until(partiValArr.length)) {
      partitions += new DolphinDBPartition(pi, addrs, partiCols, partiValArr(pi).toArray)
    }
    partitions.toArray
  }

  /**
    * A partition can be created only if the partition column is the same
    * as the filter column and the partition column values match the filter values.
    * @param partCol partition column
    * @param partiVal partition column value
    * @param partFilter filter is defined by user
    * @return
    */
  private def getDolphinDBPartitionBySingleFilter (
          partCol :String, partiVal : String,
          partFilter: Array[Filter]) : Boolean = {

    /**   Get the partiton column type in dolphindb  */
     val partType = DolphinDBRDD.originNameToType.get(partCol).get.toUpperCase

     if(partType.equals("STRING") || partType.equals("SYMBOL")) {
       var flagPart = false
       partFilter.foreach(f => f match {
           case EqualTo(attr, value) => {
             if ((attr.equalsIgnoreCase(partCol) &&
               partiVal.toString.equals(value.toString))) {
               flagPart = true
             }
           }
           case LessThan(attr, value) => {
             if ((attr.equalsIgnoreCase(partCol) &&
               partiVal.toString < value.toString)) {
               flagPart = true
             }
           }
           case GreaterThan(attr, value) => {
             if ((attr.equalsIgnoreCase(partCol) &&
               partiVal.toString > value.toString)) {
               flagPart = true
             }
           }
           case LessThanOrEqual(attr, value) => {
             if ((attr.equalsIgnoreCase(partCol) &&
               partiVal.toString <= value.toString)) {
               flagPart = true
             }
           }
           case GreaterThanOrEqual(attr, value) => {
             if ((attr.equalsIgnoreCase(partCol) &&
               partiVal.toString >= value.toString)) {
               flagPart = true
             }
           }
           case StringStartsWith(attr, value) => {
             val str = value.substring(0, attr.indexOf("%"))
             if (attr.equalsIgnoreCase(partCol) &&
               partiVal.toString.startsWith(str)) {
               flagPart = true
             }
           }
           case StringEndsWith(attr, value) => {
             val str = value.substring(attr.indexOf("%") + 1)
             if (attr.equalsIgnoreCase(partCol) &&
               partiVal.toString.endsWith(str)) {
               flagPart = true
             }
           }
           case StringContains(attr, value) => {
             val str = value.substring(1, value.lastIndexOf("%") + 1)
             if (attr.equalsIgnoreCase(partCol) &&
               partiVal.toString.contains(str)) {
               flagPart = true
             }
           }
           case In(attr, value) => {
             var vin = false
             value.foreach(v => if (v.toString.equals(partiVal)) vin = true)
             if (attr.equalsIgnoreCase(partCol) && vin) {
               flagPart = true
             }
           }
           case Not(f) => {
              if (!getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
                flagPart = true
              }
           }
           case Or(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
               getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
               flagPart = true
             }
           }
           case And(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
               getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
               flagPart = true
             }
           }
           case _ =>
       })
       flagPart
     } else if (partType.equals("INT") || partType.equals("LONG")
          || partType.equals("SHORT")){
       var flagPart = false
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal.toLong == value.toString.toLong)) {
             flagPart = true
           }
         }
         case LessThan(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal.toLong < value.toString.toLong)) {
             flagPart = true
           }
         }
         case GreaterThan(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal.toLong > value.toString.toLong)) {
             flagPart = true
           }
         }
         case LessThanOrEqual(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal.toLong <= value.toString.toLong)) {
             flagPart = true
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal.toLong >= value.toString.toLong)) {
             flagPart = true
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach(v => if (v.toString.toLong == partiVal.toLong) vin = true)
           if (attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = true
           }
         }
         case Not(f) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = true
           }
         }
         case Or(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case And(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case _ =>
       })
       flagPart
     } else if (partType.equals("CHAR")) {
       var flagPart = false
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal.charAt(0).toByte == value.toString.toByte)) {
             flagPart = true
           }
         }
         case LessThan(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal.charAt(0).toByte < value.toString.toByte)) {
             flagPart = true
           }
         }
         case GreaterThan(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal.charAt(0).toByte > value.toString.toByte)) {
             flagPart = true
           }
         }
         case LessThanOrEqual(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal.charAt(0).toByte <= value.toString.toByte)) {
             flagPart = true
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal.charAt(0).toByte >= value.toString.toByte)) {
             flagPart = true
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach(v => if (v.toString.charAt(0).toByte == partiVal.toByte) vin = true)
           if (attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = true
           }
         }
         case Not(f) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = true
           }
         }
         case Or(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case And(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case _ =>
       })
       flagPart
     } else if (partType.equals("FLOAT") || partType.equals("DOUBLE")) {
       var flagPart = false
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal.toDouble == value.toString.toDouble)) {
             flagPart = true
           }
         }
         case LessThan(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal.toDouble < value.toString.toDouble)) {
             flagPart = true
           }
         }
         case GreaterThan(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal.toDouble > value.toString.toDouble)) {
             flagPart = true
           }
         }
         case LessThanOrEqual(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal.toDouble <= value.toString.toDouble)) {
             flagPart = true
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal.toDouble >= value.toString.toDouble)) {
             flagPart = true
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach(v => if (v.toString.toDouble == partiVal.toDouble) vin = true)
           if (attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = true
           }
         }
         case Not(f) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = true
           }
         }
         case Or(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case And(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case _ =>
       })
       flagPart
     } else if (partType.equals("MONTH")) {
       var flagPart = false
       val partMon = partiVal.replace("M", "").split(".")
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           var userMonstr :String = null
           if (value.toString.contains("M")){
             userMonstr = value.toString.replace("M", "")
           } else {
             userMonstr = value.toString
           }
           val userMon = userMonstr.split("-")
           if ((attr.equalsIgnoreCase(partCol) &&
             Utils.countMonths(partMon(0).toInt, partMon(1).toInt) ==
               Utils.countMonths(userMon(0).toInt, userMon(1).toInt))) {
             flagPart = true
           }
         }
         case LessThan(attr, value) => {
           var userMonstr :String = null
           if (value.toString.contains("M")){
             userMonstr = value.toString.replace("M", "")
           } else {
             userMonstr = value.toString
           }
           val userMon = userMonstr.split("-")
           if ((attr.equalsIgnoreCase(partCol) &&
             Utils.countMonths(partMon(0).toInt, partMon(1).toInt) <
               Utils.countMonths(userMon(0).toInt, userMon(1).toInt))) {
             flagPart = true
           }
         }
         case GreaterThan(attr, value) => {
           var userMonstr :String = null
           if (value.toString.contains("M")){
             userMonstr = value.toString.replace("M", "")
           } else {
             userMonstr = value.toString
           }
           val userMon = userMonstr.split("-")
           if ((attr.equalsIgnoreCase(partCol) &&
             Utils.countMonths(partMon(0).toInt, partMon(1).toInt) >
               Utils.countMonths(userMon(0).toInt, userMon(1).toInt))) {
             flagPart = true
           }
         }
         case LessThanOrEqual(attr, value) => {
           var userMonstr :String = null
           if (value.toString.contains("M")){
             userMonstr = value.toString.replace("M", "")
           } else {
             userMonstr = value.toString
           }
           val userMon = userMonstr.split("-")
           if ((attr.equalsIgnoreCase(partCol) &&
             Utils.countMonths(partMon(0).toInt, partMon(1).toInt) <=
               Utils.countMonths(userMon(0).toInt, userMon(1).toInt))) {
             flagPart = true
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           var userMonstr :String = null
           if (value.toString.contains("M")){
             userMonstr = value.toString.replace("M", "")
           } else {
             userMonstr = value.toString
           }
           val userMon = userMonstr.split("-")
           if ((attr.equalsIgnoreCase(partCol) &&
             Utils.countMonths(partMon(0).toInt, partMon(1).toInt) >=
               Utils.countMonths(userMon(0).toInt, userMon(1).toInt))) {
             flagPart = true
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach { v =>
             var mon: String = null
             if (v.toString.contains("M")) {
               mon = v.toString.replace("M", "")
             } else {
               mon = value.toString
             }
             if (mon.replace("-", ".").contains(
                    partiVal.replace("M", ""))) {
               vin = true
             }
           }
           if (attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = true
           }
         }
         case Not(f) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = true
           }
         }
         case Or(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case And(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case _ =>
       })
       flagPart
     } else if (partType.equals("BOOL")) {
       var flagPart = false
       val partBool = if (partiVal.toString.equals("0") || partiVal.toLowerCase.equals("false")) false else true
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           val valBool =  if (value.toString.equals("0") || value.toString.toLowerCase.equals("false")) false else true
           if ((attr.equalsIgnoreCase(partCol) &&
             partiVal == valBool)) {
             flagPart = true
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach(v => {
             val vBool = if (v.toString.equals("0") || v.toString.toLowerCase.equals("false")) false else true
             if (vBool == partBool) vin = true
           })
           if (attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = true
           }
         }
         case Not(f) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = true
           }
         }
         case Or(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case And(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case _ =>
       })
       flagPart
     } else if (partType.equals("TIME")) {
       val partTimeInt = Utils.countMilliseconds(LocalTime.parse(partiVal.toString))
       var flagPart = false
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           var userTime : String = null
           if (value.toString.contains(" ") || value.toString.contains("T")) { userTime = value.toString.split("[ T]")(1) }
           else userTime = value.toString
           val userTimeInt = Utils.countMilliseconds(LocalTime.parse(userTime))
           if ((attr.equalsIgnoreCase(partCol) &&
             partTimeInt == userTimeInt)) {
             flagPart = true
           }
         }
         case LessThan(attr, value) => {
           var userTime : String = null
           if (value.toString.contains(" ") || value.toString.contains("T")) { userTime = value.toString.split("[ T]")(1) }
           else userTime = value.toString
           val userTimeInt = Utils.countMilliseconds(LocalTime.parse(userTime))
           if ((attr.equalsIgnoreCase(partCol) &&
             partTimeInt < userTimeInt)) {
             flagPart = true
           }
         }
         case GreaterThan(attr, value) => {
           var userTime : String = null
           if (value.toString.contains(" ") || value.toString.contains("T")) { userTime = value.toString.split("[ T]")(1) }
           else userTime = value.toString
           val userTimeInt = Utils.countMilliseconds(LocalTime.parse(userTime))
           if ((attr.equalsIgnoreCase(partCol) &&
             partTimeInt > userTimeInt)) {
             flagPart = true
           }
         }
         case LessThanOrEqual(attr, value) => {
           var userTime : String = null
           if (value.toString.contains(" ") || value.toString.contains("T")) { userTime = value.toString.split("[ T]")(1) }
           else userTime = value.toString
           val userTimeInt = Utils.countMilliseconds(LocalTime.parse(userTime))
           if ((attr.equalsIgnoreCase(partCol) &&
             partTimeInt <= userTimeInt)) {
             flagPart = true
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           var userTime : String = null
           if (value.toString.contains(" ") || value.toString.contains("T")) { userTime = value.toString.split("[ T]")(1) }
           else userTime = value.toString
           val userTimeInt = Utils.countMilliseconds(LocalTime.parse(userTime))
           if ((attr.equalsIgnoreCase(partCol) &&
             partTimeInt >= userTimeInt)) {
             flagPart = true
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach(v => {
             var userTime : String = null
             if (v.toString.contains(" ") || v.toString.contains("T")) { userTime = v.toString.split("[ T]")(1) }
             else userTime = v.toString
             val userTimeInt = Utils.countMilliseconds(LocalTime.parse(userTime))
             if (userTimeInt == partTimeInt) vin = true
           })
           if (attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = true
           }
         }
         case Not(f) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = true
           }
         }
         case Or(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case And(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case _ =>
       })
       flagPart
     } else if (partType.equals("MINUTE")) {
       var flagPart = false
       val partMinInt = Utils.countMinutes(LocalTime.parse(partiVal.replace("m", "")))
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           var userMin : String = null
           if (value.toString.contains(" ") || value.toString.contains("T")) { userMin = value.toString.split("[ T]")(1) }
           else userMin = value.toString
           val userMinInt = Utils.countMinutes(LocalTime.parse(userMin))
           if ((attr.equalsIgnoreCase(partCol) &&
             partMinInt == userMinInt)) {
             flagPart = true
           }
         }
         case LessThan(attr, value) => {
           var userMin : String = null
           if (value.toString.contains(" ") || value.toString.contains("T")) { userMin = value.toString.split("[ T]")(1) }
           else userMin = value.toString
           val userMinInt = Utils.countMinutes(LocalTime.parse(userMin))
           if ((attr.equalsIgnoreCase(partCol) &&
             partMinInt < userMinInt)) {
             flagPart = true
           }
         }
         case GreaterThan(attr, value) => {
           var userMin : String = null
           if (value.toString.contains(" ") || value.toString.contains("T")) { userMin = value.toString.split("[ T]")(1) }
           else userMin = value.toString
           val userMinInt = Utils.countMinutes(LocalTime.parse(userMin))
           if ((attr.equalsIgnoreCase(partCol) &&
             partMinInt > userMinInt)) {
             flagPart = true
           }
         }
         case LessThanOrEqual(attr, value) => {
           var userMin : String = null
           if (value.toString.contains(" ") || value.toString.contains("T")) { userMin = value.toString.split("[ T]")(1) }
           else userMin = value.toString
           val userMinInt = Utils.countMinutes(LocalTime.parse(userMin))
           if ((attr.equalsIgnoreCase(partCol) &&
             partMinInt <= userMinInt)) {
             flagPart = true
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           var userMin : String = null
           if (value.toString.contains(" ") || value.toString.contains("T")) { userMin = value.toString.split("[ T]")(1) }
           else userMin = value.toString
           val userMinInt = Utils.countMinutes(LocalTime.parse(userMin))
           if ((attr.equalsIgnoreCase(partCol) &&
             partMinInt >= userMinInt)) {
             flagPart = true
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach(v => {
             var userMin : String = null
             if (v.toString.contains(" ") || v.toString.contains("T")) { userMin = v.toString.split("[ T]")(1) }
             else userMin = v.toString
             val userMinInt = Utils.countMinutes(LocalTime.parse(userMin))
             if (userMinInt == partMinInt) vin = true
           })
           if (attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = true
           }
         }
         case Not(f) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = true
           }
         }
         case Or(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case And(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case _ =>
       })
       flagPart
     } else if (partType.equals("SECOND")) {
       var flagPart = false
       val partSecInt = Utils.countSeconds(LocalTime.parse(partiVal))
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           var userSec : String = null
           if (value.toString.contains(" ") || value.toString.contains("T")) { userSec = value.toString.split("[ T]")(1) }
           else userSec = value.toString
           val userSecInt = Utils.countSeconds(LocalTime.parse(userSec))
           if ((attr.equalsIgnoreCase(partCol) &&
             partSecInt == userSecInt)) {
             flagPart = true
           }
         }
         case LessThan(attr, value) => {
           var userSec : String = null
           if (value.toString.contains(" ") || value.toString.contains("T")) { userSec = value.toString.split("[ T]")(1) }
           else userSec = value.toString
           val userSecInt = Utils.countSeconds(LocalTime.parse(userSec))
           if ((attr.equalsIgnoreCase(partCol) &&
             partSecInt < userSecInt)) {
             flagPart = true
           }
         }
         case GreaterThan(attr, value) => {
           var userSec : String = null
           if (value.toString.contains(" ") || value.toString.contains("T")) { userSec = value.toString.split("[ T]")(1) }
           else userSec = value.toString
           val userSecInt = Utils.countSeconds(LocalTime.parse(userSec))
           if ((attr.equalsIgnoreCase(partCol) &&
             partSecInt > userSecInt)) {
             flagPart = true
           }
         }
         case LessThanOrEqual(attr, value) => {
           var userSec : String = null
           if (value.toString.contains(" ") || value.toString.contains("T")) { userSec = value.toString.split("[ T]")(1) }
           else userSec = value.toString
           val userSecInt = Utils.countSeconds(LocalTime.parse(userSec))
           if ((attr.equalsIgnoreCase(partCol) &&
             partSecInt <= userSecInt)) {
             flagPart = true
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           var userSec : String = null
           if (value.toString.contains(" ") || value.toString.contains("T")) { userSec = value.toString.split("[ T]")(1) }
           else userSec = value.toString
           val userSecInt = Utils.countSeconds(LocalTime.parse(userSec))
           if ((attr.equalsIgnoreCase(partCol) &&
             partSecInt >= userSecInt)) {
             flagPart = true
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach(v => {
             var userSec : String = null
             if (v.toString.contains(" ") || v.toString.contains("T")) { userSec = v.toString.split("[ T]")(1) }
             else userSec = v.toString
             val userSecInt = Utils.countSeconds(LocalTime.parse(userSec))
             if (partSecInt == userSecInt) vin = true
           })
           if (attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = true
           }
         }
         case Not(f) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = true
           }
         }
         case Or(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case And(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case _ =>
       })
       flagPart
     } else if (partType.equals("DATETIME")) {
       var flagPart = false
       val partDTInt = Utils.countSeconds(LocalDateTime.parse(partiVal.split("[ T]")(0).replace(".", "-")
                  + "T" + partiVal.split("[ T}")(1)))
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           var userDT : String = null
           if (value.toString.contains(" ")) { userDT = value.toString.replace(" ", "T") }
           else userDT = value.toString
           val userDTInt = Utils.countSeconds(LocalDateTime.parse(userDT))
           if ((attr.equalsIgnoreCase(partCol) &&
             partDTInt == userDTInt)) {
             flagPart = true
           }
         }
         case LessThan(attr, value) => {
           var userDT : String = null
           if (value.toString.contains(" ")) { userDT = value.toString.replace(" ", "T") }
           else userDT = value.toString
           val userDTInt = Utils.countSeconds(LocalDateTime.parse(userDT))
           if ((attr.equalsIgnoreCase(partCol) &&
             partDTInt < userDTInt)) {
             flagPart = true
           }
         }
         case GreaterThan(attr, value) => {
           var userDT : String = null
           if (value.toString.contains(" ")) { userDT = value.toString.replace(" ", "T") }
           else userDT = value.toString
           val userDTInt = Utils.countSeconds(LocalDateTime.parse(userDT))
           if ((attr.equalsIgnoreCase(partCol) &&
             partDTInt > userDTInt)) {
             flagPart = true
           }
         }
         case LessThanOrEqual(attr, value) => {
           var userDT : String = null
           if (value.toString.contains(" ")) { userDT = value.toString.replace(" ", "T") }
           else userDT = value.toString
           val userDTInt = Utils.countSeconds(LocalDateTime.parse(userDT))
           if ((attr.equalsIgnoreCase(partCol) &&
             partDTInt <= userDTInt)) {
             flagPart = true
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           var userDT : String = null
           if (value.toString.contains(" ")) { userDT = value.toString.replace(" ", "T") }
           else userDT = value.toString
           val userDTInt = Utils.countSeconds(LocalDateTime.parse(userDT))
           if ((attr.equalsIgnoreCase(partCol) &&
             partDTInt >= userDTInt)) {
             flagPart = true
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach(v => {
             var userDT : String = null
             if (v.toString.contains(" ")) { userDT = v.toString.replace(" ", "T") }
             else userDT = v.toString
             val userSecInt = Utils.countSeconds(LocalDateTime.parse(userDT))
             if (partDTInt == userSecInt) vin = true
           })
           if (attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = true
           }
         }
         case Not(f) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = true
           }
         }
         case Or(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case And(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case _ =>
       })
       flagPart
     } else if (partType.equals("TIMESTAMP")) {
       var flagPart = false
       val partTMInt = Utils.countMilliseconds(LocalDateTime.parse(partiVal.split("[ T]")(0).replace(".", "-")
         + "T" + partiVal.split("[ T}")(1)))
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           var userTM : String = null
           if (value.toString.contains(" ")) { userTM = value.toString.replace(" ", "T") }
           else userTM = value.toString
           val userTMInt = Utils.countMilliseconds(LocalDateTime.parse(userTM))
           if ((attr.equalsIgnoreCase(partCol) &&
             partTMInt == userTMInt)) {
             flagPart = true
           }
         }
         case LessThan(attr, value) => {
           var userTM : String = null
           if (value.toString.contains(" ")) { userTM = value.toString.replace(" ", "T") }
           else userTM = value.toString
           val userTMInt = Utils.countMilliseconds(LocalDateTime.parse(userTM))
           if ((attr.equalsIgnoreCase(partCol) &&
             partTMInt < userTMInt)) {
             flagPart = true
           }
         }
         case GreaterThan(attr, value) => {
           var userTM : String = null
           if (value.toString.contains(" ")) { userTM = value.toString.replace(" ", "T") }
           else userTM = value.toString
           val userTMInt = Utils.countMilliseconds(LocalDateTime.parse(userTM))
           if ((attr.equalsIgnoreCase(partCol) &&
             partTMInt > userTMInt)) {
             flagPart = true
           }
         }
         case LessThanOrEqual(attr, value) => {
           var userTM : String = null
           if (value.toString.contains(" ")) { userTM = value.toString.replace(" ", "T") }
           else userTM = value.toString
           val userTMInt = Utils.countMilliseconds(LocalDateTime.parse(userTM))
           if ((attr.equalsIgnoreCase(partCol) &&
             partTMInt <= userTMInt)) {
             flagPart = true
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           var userTM : String = null
           if (value.toString.contains(" ")) { userTM = value.toString.replace(" ", "T") }
           else userTM = value.toString
           val userTMInt = Utils.countMilliseconds(LocalDateTime.parse(userTM))
           if ((attr.equalsIgnoreCase(partCol) &&
             partTMInt >= userTMInt)) {
             flagPart = true
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach(v => {
             var userTM : String = null
             if (v.toString.contains(" ")) { userTM = v.toString.replace(" ", "T") }
             else userTM = v.toString
             val userTMInt = Utils.countMilliseconds(LocalDateTime.parse(userTM))
             if (partTMInt == userTMInt) vin = true
           })
           if (attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = true
           }
         }
         case Not(f) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = true
           }
         }
         case Or(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case And(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case _ =>
       })
       flagPart
     } else if (partType.equals("NANOTIME")) {
       var flagPart = false
       val partNTLong = Utils.countNanoseconds(LocalTime.parse(partiVal))
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           var userNT : String = null
           if (value.toString.contains(" ")|| value.toString.contains("T")) { userNT = value.toString.split("[ T]")(1) }
           else userNT = value.toString
           val userNTLong = Utils.countNanoseconds(LocalTime.parse(userNT))
           if ((attr.equalsIgnoreCase(partCol) &&
             partNTLong == userNTLong)) {
             flagPart = true
           }
         }
         case LessThan(attr, value) => {
           var userNT : String = null
           if (value.toString.contains(" ")|| value.toString.contains("T")) { userNT = value.toString.split("[ T]")(1) }
           else userNT = value.toString
           val userNTLong = Utils.countNanoseconds(LocalTime.parse(userNT))
           if ((attr.equalsIgnoreCase(partCol) &&
             partNTLong < userNTLong)) {
             flagPart = true
           }
         }
         case GreaterThan(attr, value) => {
           var userNT : String = null
           if (value.toString.contains(" ")|| value.toString.contains("T")) { userNT = value.toString.split("[ T]")(1) }
           else userNT = value.toString
           val userNTLong = Utils.countNanoseconds(LocalTime.parse(userNT))
           if ((attr.equalsIgnoreCase(partCol) &&
             partNTLong > userNTLong)) {
             flagPart = true
           }
         }
         case LessThanOrEqual(attr, value) => {
           var userNT : String = null
           if (value.toString.contains(" ")|| value.toString.contains("T")) { userNT = value.toString.split("[ T]")(1) }
           else userNT = value.toString
           val userNTLong = Utils.countNanoseconds(LocalTime.parse(userNT))
           if ((attr.equalsIgnoreCase(partCol) &&
             partNTLong <= userNTLong)) {
             flagPart = true
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           var userNT : String = null
           if (value.toString.contains(" ")|| value.toString.contains("T")) { userNT = value.toString.split("[ T]")(1) }
           else userNT = value.toString
           val userNTLong = Utils.countNanoseconds(LocalTime.parse(userNT))
           if ((attr.equalsIgnoreCase(partCol) &&
             partNTLong >= userNTLong)) {
             flagPart = true
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach(v => {
             var userNT : String = null
             if (v.toString.contains(" ") || value.toString.contains("T")) { userNT = v.toString.split("[ T]")(1) }
             else userNT = v.toString
             val userNTLong = Utils.countMilliseconds(LocalDateTime.parse(userNT))
             if (partNTLong == userNTLong) vin = true
           })
           if (attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = true
           }
         }
         case Not(f) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = true
           }
         }
         case Or(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case And(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case _ =>
       })
       flagPart
     } else if (partType.equals("NANOTIMESTAMP")) {
       var flagPart = false
       val partNTSLong = Utils.countNanoseconds(LocalDateTime.parse(partiVal.split("[ T]")(0).replace(".", "-")
          + "T" + partiVal.split("[ T]")(1)))
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           var userNTS : String = null
           if (value.toString.contains(" ")) { userNTS = value.toString.replace(" ", "T") }
           else userNTS = value.toString
           val userNTSLong = Utils.countNanoseconds(LocalDateTime.parse(userNTS))
           if ((attr.equalsIgnoreCase(partCol) &&
             partNTSLong == userNTSLong)) {
             flagPart = true
           }
         }
         case LessThan(attr, value) => {
           var userNTS : String = null
           if (value.toString.contains(" ")) { userNTS = value.toString.replace(" ", "T") }
           else userNTS = value.toString
           val userNTSLong = Utils.countNanoseconds(LocalDateTime.parse(userNTS))
           if ((attr.equalsIgnoreCase(partCol) &&
             partNTSLong < userNTSLong)) {
             flagPart = true
           }
         }
         case GreaterThan(attr, value) => {
           var userNTS : String = null
           if (value.toString.contains(" ")) { userNTS = value.toString.replace(" ", "T") }
           else userNTS = value.toString
           val userNTSLong = Utils.countNanoseconds(LocalDateTime.parse(userNTS))
           if ((attr.equalsIgnoreCase(partCol) &&
             partNTSLong > userNTSLong)) {
             flagPart = true
           }
         }
         case LessThanOrEqual(attr, value) => {
           var userNTS : String = null
           if (value.toString.contains(" ")) { userNTS = value.toString.replace(" ", "T") }
           else userNTS = value.toString
           val userNTSLong = Utils.countNanoseconds(LocalDateTime.parse(userNTS))
           if ((attr.equalsIgnoreCase(partCol) &&
             partNTSLong <= userNTSLong)) {
             flagPart = true
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           var userNTS : String = null
           if (value.toString.contains(" ")) { userNTS = value.toString.replace(" ", "T") }
           else userNTS = value.toString
           val userNTSLong = Utils.countNanoseconds(LocalDateTime.parse(userNTS))
           if ((attr.equalsIgnoreCase(partCol) &&
             partNTSLong >= userNTSLong)) {
             flagPart = true
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach(v => {
             var userNTS : String = null
             if (v.toString.contains(" ")) { userNTS = v.toString.replace(" ", "T") }
             else userNTS = v.toString
             val userNTSLong = Utils.countNanoseconds(LocalDateTime.parse(userNTS))
             if (partNTSLong == userNTSLong) vin = true
           })
           if (attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = true
           }
         }
         case Not(f) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = true
           }
         }
         case Or(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case And(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case _ =>
       })
       flagPart
     } else if (partType.equals("DATE")) {
       var flagPart = false
       val partDInt = Utils.countDays(LocalDate.parse(partiVal.replace(".", "-")))
       partFilter.foreach(f => f match {
         case EqualTo(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partDInt == Utils.countDays(LocalDate.parse(value.toString)))) {
             flagPart = true
           }
         }
         case LessThan(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partDInt < Utils.countDays(LocalDate.parse(value.toString)))) {
             flagPart = true
           }
         }
         case GreaterThan(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partDInt > Utils.countDays(LocalDate.parse(value.toString)))) {
             flagPart = true
           }
         }
         case LessThanOrEqual(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partDInt <= Utils.countDays(LocalDate.parse(value.toString)))) {
             flagPart = true
           }
         }
         case GreaterThanOrEqual(attr, value) => {
           if ((attr.equalsIgnoreCase(partCol) &&
             partDInt >= Utils.countDays(LocalDate.parse(value.toString)))) {
             flagPart = true
           }
         }
         case In(attr, value) => {
           var vin = false
           value.foreach(v => {
             if (partDInt == Utils.countDays(LocalDate.parse(v.toString))) vin = true
           })
           if (attr.equalsIgnoreCase(partCol) && vin) {
             flagPart = true
           }
         }
         case Not(f) => {
           if (!getDolphinDBPartitionBySingleFilter(partCol,partiVal,Array(f))) {
             flagPart = true
           }
         }
         case Or(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) ||
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case And(f1, f2) => {
           if (getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f1)) &&
             getDolphinDBPartitionBySingleFilter(partCol, partiVal, Array(f2))) {
             flagPart = true
           }
         }
         case _ =>
       })
       flagPart
     } else {
       false
     }
  }


}
