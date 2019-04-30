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
      partitions += new DolphinDBPartition(0, null, null, null,null)
      return partitions.toArray
    }

    val partiType : Array[Int] = DolphinDBSchema.getPartitionType(conn, option)
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
    var partiValArr = new ArrayBuffer[ArrayBuffer[Array[String]]]()  // Contains all partition value in Spark
//    var partiValArr = new ArrayBuffer[ArrayBuffer[String]]()  // Contains all partition value in Spark
      /** There are multiple partitions in DolphinDB table */
    if (/*partiVals.isInstanceOf[BasicAnyVector] &&*/ partiCols.length > 1) {
      val partiVector = partiVals.asInstanceOf[BasicAnyVector]

      for (i <- 0 until(partiVector.rows())) {
        val tmpPartiVal = new ArrayBuffer[ArrayBuffer[Array[String]]]()
        val vector = partiVector.getEntity(i).asInstanceOf[Vector]
        for (j <- 0 until(vector.rows())) {
          val vecBuf = new ArrayBuffer[String]()

          /**  There is a partition type of 3 , 3 represent List partition   */
          if (vector.isInstanceOf[BasicAnyVector] && partiType(i) == 3) {
            val vectorAny = vector.asInstanceOf[BasicAnyVector]
            if (vectorAny.getEntity(j).isInstanceOf[Vector] ) {
              val vectorVals = vectorAny.getEntity(j).asInstanceOf[Vector]
              for (k <- 0 until(vectorVals.rows())) {
                vecBuf += vectorVals.get(k).toString
              }
            } else {   /**  there is a scalar value in List partition's BasicAnyVector  */
              vecBuf += vectorAny.get(j).toString
            }
          } else if (partiType(i) == 2){  /**  There is a partition type of 2 , 2 = Range partition    */
            if (j != vector.rows() - 1) {
              vecBuf += vector.get(j).toString
              vecBuf += vector.get(j+1).toString
            } else {
              vecBuf += vector.get(j).toString
              vecBuf += vector.get(j).toString
            }
          } else {
            vecBuf += vector.get(j).toString
          }


          /**  i = 0 means the first level partition in DolphinDB   */
          if (i == 0) {

            /** There are partition filter in User-defined SQL   */
            if (partiFilters.length != 0 && getDolphinDBPartitionBySingleFilter(partiCols(i),
              vecBuf.toArray, partiType(i),partiFilters.toArray)) {
              partiValArr += ArrayBuffer[Array[String]](vecBuf.toArray)
            } else {
            /**  There are not partition filter in User-defined SQL */
              partiValArr += ArrayBuffer[Array[String]](vecBuf.toArray)
            }
          } else {

            /**  means not the first level partition in DolphinDB */
            var addPartFlag = true
            /** There are partition filter in User-defined SQL   */
            if (!(partiFilters.length != 0 && getDolphinDBPartitionBySingleFilter(partiCols(i),
              vecBuf.toArray, partiType(i), partiFilters.toArray))){
              addPartFlag = false
            }
            if (addPartFlag) {
              for (pv <- 0 until (partiValArr.size)) {
                val tmpBuf = new ArrayBuffer[Array[String]]()
                partiValArr(pv).copyToBuffer(tmpBuf)
                tmpBuf += vecBuf.toArray
                tmpPartiVal += tmpBuf
              }
            }
          }

        }
        partiValArr = tmpPartiVal
        tmpPartiVal.clear()
      }
    } else {
      /** There is only one partition in DolphinDB table. */
      /////////////////////////////////////////////////////////
      /** There are not partition filter in User-defined SQL */

        for (pi <- 0 until (partiVals.rows())) {
          var addPartFlag = true
          val partiBuf = new ArrayBuffer[String]()
          /**  partitionType = 3 stands for list partition
            *  if [[12,34],[12,346]] or [[12,23], [34]] must transform to BasicAnyVector
            * */
          if (partiVals.isInstanceOf[BasicAnyVector] && partiType(0) == 3) {
            val partiListVals = partiVals.asInstanceOf[BasicAnyVector]

            if (partiListVals.getEntity(pi).isInstanceOf[Vector]) {
              val partiListVector = partiListVals.getEntity(pi).asInstanceOf[Vector]
              for (plv <- 0 until(partiListVector.rows())) {
                partiBuf += partiListVector.get(plv).toString
              }
            } else {
              /**    partiListVals type is BasicAnyVector but contain Scalar    */
              partiBuf += partiListVals.get(pi).toString
            }
          } else if (partiType(0) == 2) {  /**  partitionType = 2 stands for range partition */
            if (pi != partiVals.rows() - 1) {
              partiBuf += partiVals.get(pi).toString
              partiBuf += partiVals.get(pi+1).toString
            } else {
              partiBuf += partiVals.get(pi).toString
              partiBuf += partiVals.get(pi).toString
            }
          } else {
            partiBuf += partiVals.get(pi).toString
          }
          /** There are partition filter in User-defined SQL, So filter partition  */
          if (!(partiFilters.length != 0 && getDolphinDBPartitionBySingleFilter(partiCols(0),
            partiBuf.toArray, partiType(0), partiFilters.toArray))) {
            addPartFlag = false
          }
          if (addPartFlag) {
            partiValArr += ArrayBuffer(partiBuf.toArray)
          }
      }
    }

    /**   Create spark partitions based on filtered partition values     */
    for (pi <- 0 until(partiValArr.length)) {
      partitions += new DolphinDBPartition(pi, addrs, partiCols,partiType, partiValArr(pi).toArray)
    }
    partitions.toArray
  }

  /**
    * A partition can be created only if the partition column is the same
    * as the filter column and the partition column values match the filter values.
    * @param partCol partition column
    * @param partiVals partition column value
    * @param partiType partition type
    * @param partFilter filter is defined by user
    * @return
    */
  private def getDolphinDBPartitionBySingleFilter (
          partCol :String, partiVals : Array[String], partiType : Int,
          partFilter: Array[Filter]) : Boolean = {

    /**   Get the partiton column type in dolphindb  */
    val partType = DolphinDBRDD.originNameToType.get(partCol).get.toUpperCase

    var flagPart = false
//     for (partiVal <- partiVals) {
       if(partType.equals("STRING") || partType.equals("SYMBOL")) {
         partFilter.foreach(f => f match {
             case EqualTo(attr, value) => {
               var typeFlag = false
               if (partType == 3) {
                partiVals.foreach(pv => if (pv.equals(value.toString)) {
                  typeFlag = true
                })
               } else if (partType == 2) {
                 if (partiVals(0).equals(partiVals(1)) && partiVals(0).equals(value.toString)){
                    typeFlag = true
                 } else if((partiVals(0) <= value.toString && value.toString < partiVals(1)) ||
                   (partiVals(1) <= value.toString && value.toString < partiVals(0))){
                   typeFlag = true
                 }
               } else {
                if (partiVals(0).equals(value.toString)) typeFlag = true
               }
               if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
                 flagPart = true
               }
             }
             case LessThan(attr, value) => {
               var typeFlag = false
               if (partType == 3) {
                 partiVals.foreach(pv => if (pv < value.toString) {
                   typeFlag = true
                 })
                } else if (partType == 2) {
                   if((partiVals(0) < value.toString || partiVals(1) < value.toString) ){
                   typeFlag = true
                 }
               } else {
                 if (partiVals(0) < (value.toString)) typeFlag = true
               }
               if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
                 flagPart = true
               }
             }
             case GreaterThan(attr, value) => {
               var typeFlag = false
               if (partType == 3) {
                 partiVals.foreach(pv => if (pv > value.toString) {
                   typeFlag = true
                 })
               } else if (partType == 2) {
                 if((partiVals(0) > value.toString || partiVals(1) > value.toString) ){
                   typeFlag = true
                 }
               } else {
                 if (partiVals(0) > (value.toString)) typeFlag = true
               }
               if ((attr.equalsIgnoreCase(partCol) &&typeFlag)) {
                 flagPart = true
               }
             }
             case LessThanOrEqual(attr, value) => {
               var typeFlag = false
               if (partType == 3) {
                 partiVals.foreach(pv => if (pv <= value.toString) {
                   typeFlag = true
                 })
               } else if (partType == 2) {
                 if((partiVals(0) <= value.toString || partiVals(1) <= value.toString) ){
                   typeFlag = true
                 }
               } else {
                 if (partiVals(0) <= (value.toString)) typeFlag = true
               }
               if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
                 flagPart = true
               }
             }
             case GreaterThanOrEqual(attr, value) => {
               var typeFlag = false
               if (partType == 3) {
                 partiVals.foreach(pv => if (pv >= value.toString) {
                   typeFlag = true
                 })
               } else if (partType == 2) {
                 if (partiVals(0).equals(partiVals(1)) && partiVals(0).equals(value.toString)){
                   typeFlag = true
                 }
                 if((partiVals(0) >= value.toString || partiVals(1) >= value.toString) ){
                   typeFlag = true
                 }
               } else {
                 if (partiVals(0) <= (value.toString)) typeFlag = true
               }
               if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
                 flagPart = true
               }
             }
             case StringStartsWith(attr, value) => {
               var typeFlag = false
               val userStr = value.replace("%", "").replace("*", "")
               if (partType == 3) {
                 partiVals.foreach(pv => if (pv.toString.startsWith(userStr)) {
                   typeFlag = true
                 })
               } else if (partType == 2) {
                 if((partiVals(0) >= userStr.toString || partiVals(1) <= userStr.toString) ||
                   (partiVals(1) >= userStr.toString || partiVals(0) <= userStr.toString)){
                   typeFlag = true
                 }
               } else {
                 if (partiVals(0).toString.startsWith(userStr)) typeFlag = true
               }

               if (attr.equalsIgnoreCase(partCol) && typeFlag) {
                 flagPart = true
               }
             }
             case StringEndsWith(attr, value) => {
               var typeFlag = false
               val userStr = value.replace("%", "").replace("*", "")
               if (partType == 3) {
                 partiVals.foreach(pv => if (pv.toString.endsWith(userStr)) {
                   typeFlag = true
                 })
               } else if (partType == 2) {
//                 if((partiVals(0) >= userStr.toString || partiVals(1) <= userStr.toString) ||
//                   (partiVals(1) >= userStr.toString || partiVals(0) <= userStr.toString)){
                   typeFlag = true
//                 }
               } else {
                 if (partiVals(0).toString.endsWith(userStr)) typeFlag = true
               }

               if (attr.equalsIgnoreCase(partCol) && typeFlag ) {
                 flagPart = true
               }
             }
             case StringContains(attr, value) => {
               var typeFlag = false
               val userStr = value.replace("%", "").replace("*", "")
               if (partType == 3) {
                 partiVals.foreach(pv => if (pv.toString.contains(userStr)) {
                   typeFlag = true
                 })
               } else if (partType == 2) {
//                 if((partiVals(0) >= userStr.toString || partiVals(1) <= userStr.toString) ||
//                      (partiVals(1) >= userStr.toString || partiVals(0) <= userStr.toString)){
                   typeFlag = true
//                 }
               } else {
                 if (partiVals(0).toString.endsWith(userStr)) typeFlag = true
               }

               if (attr.equalsIgnoreCase(partCol) && typeFlag) {
                 flagPart = true
               }
             }
             case In(attr, value) => {
               var typeFlag = false

               value.foreach(v => {
//                 if (v.toString.equals(partiVals)) typeFlag = true
                 if (partType == 3) {
                   partiVals.foreach(pv => if (pv.toString.equals(v.toString)) {
                     typeFlag = true
                   })
                 } else if (partType == 2) {
                   if ((partiVals(0) <= v.toString && v.toString <= partiVals(1)) ||
                     (partiVals(1) <= v.toString && v.toString <= partiVals(0))) {
                     typeFlag = true
                   }
                 } else {
                   if (v.toString.equals(partiVals(0))) typeFlag = true
                 }
               })
               if (attr.equalsIgnoreCase(partCol) && typeFlag) {
                 flagPart = true
               }
             }
             case Not(f) => {
                if (!getDolphinDBPartitionBySingleFilter(partCol, partiVals, partiType,Array(f))) {
                  flagPart = true
                }
             }
             case Or(f1, f2) => {
               if (getDolphinDBPartitionBySingleFilter(partCol, partiVals, partiType, Array(f1)) ||
                 getDolphinDBPartitionBySingleFilter(partCol, partiVals, partiType, Array(f2))) {
                 flagPart = true
               }
             }
             case And(f1, f2) => {
               if (getDolphinDBPartitionBySingleFilter(partCol, partiVals, partiType, Array(f1)) &&
                 getDolphinDBPartitionBySingleFilter(partCol, partiVals, partiType, Array(f2))) {
                 flagPart = true
               }
             }
             case _ =>
         })
       } else if (partType.equals("INT") || partType.equals("LONG")
            || partType.equals("SHORT")){
         partFilter.foreach(f => f match {
           case EqualTo(attr, value) => {
             var typeFlag = false
             if (partType == 3) {
               partiVals.foreach(pv => if (pv.toLong == value.toString.toLong) {
                 typeFlag = true
               })
             } else if (partType == 2) {
               if (partiVals(0).toLong == partiVals(1).toLong && partiVals(0).toLong == value.toString.toLong){
                 typeFlag = true
               } else if((partiVals(0).toLong <= value.toString.toLong && value.toString.toLong < partiVals(1).toLong) ||
                 (partiVals(1).toLong <= value.toString.toLong && value.toString.toLong < partiVals(0).toLong)){
                 typeFlag = true
               }
             } else {
               if (partiVals(0).toLong == value.toString.toLong) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThan(attr, value) => {
             var typeFlag = false
             if (partType == 3) {
               partiVals.foreach(pv => if (pv.toLong < value.toString.toLong) {
                 typeFlag = true
               })
             } else if (partType == 2) {
               if((partiVals(0).toLong < value.toString.toLong || partiVals(1).toLong < value.toString.toLong)){
                 typeFlag = true
               }
             } else {
               if (partiVals(0).toLong < value.toString.toLong) typeFlag = true
             }
             if (attr.equalsIgnoreCase(partCol) && typeFlag) {
               flagPart = true
             }
           }
           case GreaterThan(attr, value) => {
             var typeFlag = false
             if (partType == 3) {
               partiVals.foreach(pv => if (pv.toLong > value.toString.toLong) {
                 typeFlag = true
               })
             } else if (partType == 2) {
               if((partiVals(0).toLong > value.toString.toLong || value.toString.toLong < partiVals(1).toLong)){
                 typeFlag = true
               }
             } else {
               if (partiVals(0).toLong > value.toString.toLong) typeFlag = true
             }
             if (attr.equalsIgnoreCase(partCol) && typeFlag) {
               flagPart = true
             }
           }
           case LessThanOrEqual(attr, value) => {
             var typeFlag = false
             if (partType == 3) {
               partiVals.foreach(pv => if (pv.toLong <= value.toString.toLong) {
                 typeFlag = true
               })
             } else if (partType == 2) {
               if((partiVals(0).toLong <= value.toString.toLong || partiVals(1).toLong  <=  value.toString.toLong)){
                 typeFlag = true
               }
             } else {
               if (partiVals(0).toLong <= value.toString.toLong) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThanOrEqual(attr, value) => {
             var typeFlag = false
             if (partType == 3) {
               partiVals.foreach(pv => if (pv.toLong >= value.toString.toLong) {
                 typeFlag = true
               })
             } else if (partType == 2) {
               if((partiVals(0).toLong >= value.toString.toLong || value.toString.toLong <= partiVals(1).toLong)){
                 typeFlag = true
               }
             } else {
               if (partiVals(0).toLong >= value.toString.toLong) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case In(attr, value) => {
             var typeFlag = false
             value.foreach(v => {
               if (partType == 3) {
                 partiVals.foreach(pv => if (pv.toLong == v.toString.toLong) {
                   typeFlag = true
                 })
               } else if (partType == 2) {
                 if((partiVals(0).toLong <= value.toString.toLong && value.toString.toLong <= partiVals(1).toLong) ||
                    (partiVals(1).toLong <= value.toString.toLong && value.toString.toLong <= partiVals(0).toLong)){
                   typeFlag = true
                 }
               } else {
                 if (partiVals(0).toLong == value.toString.toLong) typeFlag = true
               }
             })
             if (attr.equalsIgnoreCase(partCol) && typeFlag) {
               flagPart = true
             }
           }
           case Not(f) => {
             if (!getDolphinDBPartitionBySingleFilter(partCol,partiVals,partiType,Array(f))) {
               flagPart = true
             }
           }
           case Or(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) ||
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case And(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) &&
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case _ =>
         })
       } else if (partType.equals("CHAR")) {
         partFilter.foreach(f => f match {
           case EqualTo(attr, value) => {
             var typeFlag = false
             if (partType == 3) {
               partiVals.foreach(pv => if (pv.charAt(0).toByte == value.toString.charAt(0).toByte) {
                 typeFlag = true
               })
             } else if (partType == 2) {
               if (partiVals(0).charAt(0).toByte == partiVals(1).charAt(0).toByte &&
                  partiVals(0).charAt(0).toByte == value.toString.charAt(0).toByte){
                 typeFlag = true
               } else if((partiVals(0).charAt(0).toByte <= value.toString.charAt(0).toByte && value.toString.charAt(0).toByte < partiVals(1).charAt(0).toByte) ||
                 (partiVals(1).charAt(0).toByte <= value.toString.charAt(0).toByte && value.toString.charAt(0).toByte < partiVals(0).charAt(0).toByte)){
                 typeFlag = true
               }
             } else {
               if (partiVals(0).charAt(0).toByte == value.toString.charAt(0).toByte) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThan(attr, value) => {
             var typeFlag = false
             if (partType == 3) {
               partiVals.foreach(pv => if (pv.charAt(0).toByte < value.toString.charAt(0).toByte) {
                 typeFlag = true
               })
             } else if (partType == 2) {
               if((partiVals(0).charAt(0).toByte < value.toString.charAt(0).toByte || partiVals(1).charAt(0).toByte < value.toString.charAt(0).toByte)){
                 typeFlag = true
               }
             } else {
               if (partiVals(0).charAt(0).toByte < value.toString.charAt(0).toByte) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThan(attr, value) => {
             var typeFlag = false
             if (partType == 3) {
               partiVals.foreach(pv => if (pv.charAt(0).toByte > value.toString.charAt(0).toByte) {
                 typeFlag = true
               })
             } else if (partType == 2) {
               if((partiVals(0).charAt(0).toByte > value.toString.charAt(0).toByte || partiVals(1).charAt(0).toByte > value.toString.charAt(0).toByte)){
                 typeFlag = true
               }
             } else {
               if (partiVals(0).charAt(0).toByte > value.toString.charAt(0).toByte) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThanOrEqual(attr, value) => {
             var typeFlag = false
             if (partType == 3) {
               partiVals.foreach(pv => if (pv.charAt(0).toByte <= value.toString.charAt(0).toByte) {
                 typeFlag = true
               })
             } else if (partType == 2) {
               if((partiVals(0).charAt(0).toByte <= value.toString.charAt(0).toByte || partiVals(1).charAt(0).toByte <= value.toString.charAt(0).toByte)){
                 typeFlag = true
               }
             } else {
               if (partiVals(0).charAt(0).toByte <= value.toString.charAt(0).toByte) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThanOrEqual(attr, value) => {
             var typeFlag = false
             if (partType == 3) {
               partiVals.foreach(pv => if (pv.charAt(0).toByte >= value.toString.charAt(0).toByte) {
                 typeFlag = true
               })
             } else if (partType == 2) {
               if((partiVals(0).charAt(0).toByte >= value.toString.charAt(0).toByte || partiVals(1).charAt(0).toByte >= value.toString.charAt(0).toByte)){
                 typeFlag = true
               }
             } else {
               if (partiVals(0).charAt(0).toByte >= value.toString.charAt(0).toByte) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case In(attr, value) => {
             var typeFlag = false
             value.foreach(v => {
               if (partType == 3) {
                 partiVals.foreach(pv => if (pv.charAt(0).toByte == v.toString.charAt(0).toByte) {
                   typeFlag = true
                 })
               } else if (partType == 2) {
                 if((partiVals(0).charAt(0).toByte <= v.toString.charAt(0).toByte || partiVals(1).charAt(0).toByte >= v.toString.charAt(0).toByte) ||
                      (partiVals(0).charAt(0).toByte >= v.toString.charAt(0).toByte || partiVals(1).charAt(0).toByte <= v.toString.charAt(0).toByte)){
                   typeFlag = true
                 }
               } else {
                 if (partiVals(0).charAt(0).toByte == v.toString.charAt(0).toByte) typeFlag = true
               }
             })
             if (attr.equalsIgnoreCase(partCol) && typeFlag) {
               flagPart = true
             }
           }
           case Not(f) => {
             if (!getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType,Array(f))) {
               flagPart = true
             }
           }
           case Or(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) ||
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case And(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) &&
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case _ =>
         })
       } else if (partType.equals("FLOAT") || partType.equals("DOUBLE")) {
         partFilter.foreach(f => f match {
           case EqualTo(attr, value) => {
             var typeFlag = false
             if (partType == 3) {
               partiVals.foreach(pv => if (pv.toDouble == value.toString.toDouble) {
                 typeFlag = true
               })
             } else if (partType == 2) {
               if (partiVals(0).toDouble == partiVals(1).toDouble && partiVals(0).toDouble == value.toString.toDouble){
                 typeFlag = true
               } else if((partiVals(0).toDouble <= value.toString.toDouble && value.toString.toDouble < partiVals(1).toDouble) ||
                 (partiVals(1).toDouble <= value.toString.toDouble && value.toString.toDouble < partiVals(0).toDouble)){
                 typeFlag = true
               }
             } else {
               if (partiVals(0).toDouble == value.toString.toDouble) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThan(attr, value) => {
             var typeFlag = false
             if (partType == 3) {
               partiVals.foreach(pv => if (pv.toDouble < value.toString.toDouble) {
                 typeFlag = true
               })
             } else if (partType == 2) {
               if((partiVals(0).toDouble < value.toString.toDouble || partiVals(1).toDouble < value.toString.toDouble)){
                 typeFlag = true
               }
             } else {
               if (partiVals(0).toDouble < value.toString.toDouble) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag )) {
               flagPart = true
             }
           }
           case GreaterThan(attr, value) => {
             var typeFlag = false
             if (partType == 3) {
               partiVals.foreach(pv => if (pv.toDouble > value.toString.toDouble) {
                 typeFlag = true
               })
             } else if (partType == 2) {
               if((partiVals(0).toDouble > value.toString.toDouble || partiVals(1).toDouble > value.toString.toDouble)){
                 typeFlag = true
               }
             } else {
               if (partiVals(0).toDouble > value.toString.toDouble) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThanOrEqual(attr, value) => {
             var typeFlag = false
             if (partType == 3) {
               partiVals.foreach(pv => if (pv.toDouble <= value.toString.toDouble) {
                 typeFlag = true
               })
             } else if (partType == 2) {
               if((partiVals(0).toDouble <= value.toString.toDouble || partiVals(1).toDouble <= value.toString.toDouble)){
                 typeFlag = true
               }
             } else {
               if (partiVals(0).toDouble <= value.toString.toDouble) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThanOrEqual(attr, value) => {
             var typeFlag = false
             if (partType == 3) {
               partiVals.foreach(pv => if (pv.toDouble >= value.toString.toDouble) {
                 typeFlag = true
               })
             } else if (partType == 2) {
               if((partiVals(0).toDouble >= value.toString.toDouble || partiVals(1).toDouble >= value.toString.toDouble)){
                 typeFlag = true
               }
             } else {
               if (partiVals(0).toDouble >= value.toString.toDouble) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case In(attr, value) => {
             var typeFlag = false
             value.foreach(v => {
               if (partType == 3) {
                 partiVals.foreach(pv => if (pv.toDouble == v.toString.toDouble) {
                   typeFlag = true
                 })
               } else if (partType == 2) {
                 if((partiVals(0).toDouble <= value.toString.toDouble && value.toString.toDouble <= partiVals(1).toDouble) ||
                   (partiVals(1).toDouble <= value.toString.toDouble && value.toString.toDouble <= partiVals(0).toDouble)){
                   typeFlag = true
                 }
               } else {
                 if (partiVals(0).toDouble == value.toString.toDouble) typeFlag = true
               }
             })
             if (attr.equalsIgnoreCase(partCol) && typeFlag) {
               flagPart = true
             }
           }
           case Not(f) => {
             if (!getDolphinDBPartitionBySingleFilter(partCol,partiVals,partiType,Array(f))) {
               flagPart = true
             }
           }
           case Or(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) ||
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case And(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) &&
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case _ =>
         })
       } else if (partType.equals("MONTH")) {
         partFilter.foreach(f => f match {
           case EqualTo(attr, value) => {
             var typeFlag = false
             var userMonstr :String = null
             if (value.toString.contains("M"))  userMonstr = value.toString.replace("M", "")
             else userMonstr = value.toString
             val userMon = userMonstr.split("-")

             if (partType == 3){
               partiVals.foreach(pv =>{
                 val partMon = pv.replace("M", "").split(".")
                 if (Utils.countMonths(partMon(0).toInt, partMon(1).toInt) ==
                   Utils.countMonths(userMon(0).toInt, userMon(1).toInt)) typeFlag = true
               })
             } else if (partType == 2) {
               val partMon0 = partiVals(0).replace("M", "").split(".")
               val partMon1 = partiVals(1).replace("M", "").split(".")
               if ((partMon0(0).toInt == partMon1(0).toInt && partMon0(1).toInt == partMon1(1).toInt &&
                 Utils.countMonths(partMon0(0).toInt, partMon0(1).toInt) ==
                   Utils.countMonths(userMon(0).toInt, userMon(1).toInt))) typeFlag = true
               else if ((Utils.countMonths(partMon0(0).toInt, partMon0(1).toInt) <= Utils.countMonths(userMon(0).toInt, userMon(1).toInt) &&
                     Utils.countMonths(userMon(0).toInt, userMon(1).toInt) < Utils.countMonths(partMon1(0).toInt, partMon1(1).toInt)) ||
                 (Utils.countMonths(partMon0(0).toInt, partMon0(1).toInt) >= Utils.countMonths(userMon(0).toInt, userMon(1).toInt) &&
                   Utils.countMonths(userMon(0).toInt, userMon(1).toInt) > Utils.countMonths(partMon1(0).toInt, partMon1(1).toInt))) typeFlag = true
             } else {
               val partMon = partiVals(0).replace("M", "").split(".")
               if (Utils.countMonths(partMon(0).toInt, partMon(1).toInt) ==
                 Utils.countMonths(userMon(0).toInt, userMon(1).toInt)) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThan(attr, value) => {
             var typeFlag = false
             var userMonstr :String = null
             if (value.toString.contains("M")){
               userMonstr = value.toString.replace("M", "")
             } else {
               userMonstr = value.toString
             }
             val userMon = userMonstr.split("-")

             if (partType == 3){
               partiVals.foreach(pv =>{
                 val partMon = pv.replace("M", "").split(".")
                 if (Utils.countMonths(partMon(0).toInt, partMon(1).toInt) <
                   Utils.countMonths(userMon(0).toInt, userMon(1).toInt)) typeFlag = true
               })
             } else if (partType == 2) {
               val partMon0 = partiVals(0).replace("M", "").split(".")
               val partMon1 = partiVals(1).replace("M", "").split(".")
               if ((Utils.countMonths(partMon0(0).toInt, partMon0(1).toInt) < Utils.countMonths(userMon(0).toInt, userMon(1).toInt) ||
                 Utils.countMonths(partMon1(0).toInt, partMon1(1).toInt) < Utils.countMonths(userMon(0).toInt, userMon(1).toInt)))
                  typeFlag = true
             } else {
               val partMon = partiVals(0).replace("M", "").split(".")
               if (Utils.countMonths(partMon(0).toInt, partMon(1).toInt) <
                 Utils.countMonths(userMon(0).toInt, userMon(1).toInt)) typeFlag = true
             }

             if (attr.equalsIgnoreCase(partCol) && typeFlag) {
               flagPart = true
             }
           }
           case GreaterThan(attr, value) => {
             var userMonstr :String = null
             var typeFlag = false
             if (value.toString.contains("M")){
               userMonstr = value.toString.replace("M", "")
             } else {
               userMonstr = value.toString
             }
             val userMon = userMonstr.split("-")

             if (partType == 3){
               partiVals.foreach(pv =>{
                 val partMon = pv.replace("M", "").split(".")
                 if (Utils.countMonths(partMon(0).toInt, partMon(1).toInt) >
                   Utils.countMonths(userMon(0).toInt, userMon(1).toInt)) typeFlag = true
               })
             } else if (partType == 2) {
               val partMon0 = partiVals(0).replace("M", "").split(".")
               val partMon1 = partiVals(1).replace("M", "").split(".")
               if ((Utils.countMonths(partMon0(0).toInt, partMon0(1).toInt) > Utils.countMonths(userMon(0).toInt, userMon(1).toInt) ||
                 Utils.countMonths(partMon1(0).toInt, partMon1(1).toInt) > Utils.countMonths(userMon(0).toInt, userMon(1).toInt)))
                 typeFlag = true
             } else {
               val partMon = partiVals(0).replace("M", "").split(".")
               if (Utils.countMonths(partMon(0).toInt, partMon(1).toInt) >
                 Utils.countMonths(userMon(0).toInt, userMon(1).toInt)) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThanOrEqual(attr, value) => {
             var typeFlag = false
             var userMonstr :String = null
             if (value.toString.contains("M")){
               userMonstr = value.toString.replace("M", "")
             } else {
               userMonstr = value.toString
             }
             val userMon = userMonstr.split("-")

             if (partType == 3){
               partiVals.foreach(pv =>{
                 val partMon = pv.replace("M", "").split(".")
                 if (Utils.countMonths(partMon(0).toInt, partMon(1).toInt) <=
                   Utils.countMonths(userMon(0).toInt, userMon(1).toInt)) typeFlag = true
               })
             } else if (partType == 2) {
               val partMon0 = partiVals(0).replace("M", "").split(".")
               val partMon1 = partiVals(1).replace("M", "").split(".")
               if ((Utils.countMonths(partMon0(0).toInt, partMon0(1).toInt) <= Utils.countMonths(userMon(0).toInt, userMon(1).toInt) ||
                 Utils.countMonths(partMon1(0).toInt, partMon1(1).toInt) <= Utils.countMonths(userMon(0).toInt, userMon(1).toInt)))
                 typeFlag = true
             } else {
               val partMon = partiVals(0).replace("M", "").split(".")
               if (Utils.countMonths(partMon(0).toInt, partMon(1).toInt) <=
                 Utils.countMonths(userMon(0).toInt, userMon(1).toInt)) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag )) {
               flagPart = true
             }
           }
           case GreaterThanOrEqual(attr, value) => {
             var userMonstr :String = null
             var typeFlag = false
             if (value.toString.contains("M")){
               userMonstr = value.toString.replace("M", "")
             } else {
               userMonstr = value.toString
             }
             val userMon = userMonstr.split("-")

             if (partType == 3){
               partiVals.foreach(pv =>{
                 val partMon = pv.replace("M", "").split(".")
                 if (Utils.countMonths(partMon(0).toInt, partMon(1).toInt) >=
                   Utils.countMonths(userMon(0).toInt, userMon(1).toInt)) typeFlag = true
               })
             } else if (partType == 2) {
               val partMon0 = partiVals(0).replace("M", "").split(".")
               val partMon1 = partiVals(1).replace("M", "").split(".")
               if ((Utils.countMonths(partMon0(0).toInt, partMon0(1).toInt) >= Utils.countMonths(userMon(0).toInt, userMon(1).toInt) ||
                 Utils.countMonths(partMon1(0).toInt, partMon1(1).toInt) >= Utils.countMonths(userMon(0).toInt, userMon(1).toInt)))
                 typeFlag = true
             } else {
               val partMon = partiVals(0).replace("M", "").split(".")
               if (Utils.countMonths(partMon(0).toInt, partMon(1).toInt) >=
                 Utils.countMonths(userMon(0).toInt, userMon(1).toInt)) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case In(attr, value) => {
             var typeFlag = false
             value.foreach { v =>
               var mon: String = null
               if (v.toString.contains("M")) {
                 mon = v.toString.replace("M", "")
               } else {
                 mon = v.toString
               }
               val monInt = mon.split("-")

               if (partType == 3){
                 partiVals.foreach(pv =>{
                   val partMon = pv.replace("M", "").split(".")
                   if (Utils.countMonths(monInt(0).toInt, monInt(1).toInt) ==
                      Utils.countMonths(partMon(0).toInt, partMon(1).toInt)) typeFlag = true
                 })
               } else if (partType == 2) {
                 val partMon0 = partiVals(0).replace("M", "").split(".")
                 val partMon1 = partiVals(1).replace("M", "").split(".")
                 if (partiVals(0).equals(partiVals(1)) && Utils.countMonths(partMon0(0).toInt, partMon0(1).toInt) ==
                   Utils.countMonths(monInt(0).toInt, monInt(1).toInt)) typeFlag = true
                 else if ((Utils.countMonths(partMon0(0).toInt, partMon0(1).toInt) <= Utils.countMonths(monInt(0).toInt, monInt(1).toInt) &&
                   Utils.countMonths(monInt(0).toInt, monInt(1).toInt) < Utils.countMonths(partMon1(0).toInt, partMon1(1).toInt)) ||
                   (Utils.countMonths(partMon0(0).toInt, partMon0(1).toInt) > Utils.countMonths(monInt(0).toInt, monInt(1).toInt) &&
                     Utils.countMonths(monInt(0).toInt, monInt(1).toInt) >= Utils.countMonths(partMon1(0).toInt, partMon1(1).toInt)))
                   typeFlag = true
               } else {
                 val partMon = partiVals(0).replace("M", "").split(".")
                 if (Utils.countMonths(partMon(0).toInt, partMon(1).toInt) ==
                   Utils.countMonths(monInt(0).toInt, monInt(1).toInt)) typeFlag = true
               }
             }
             if (attr.equalsIgnoreCase(partCol) && typeFlag) {
               flagPart = true
             }
           }
           case Not(f) => {
             if (!getDolphinDBPartitionBySingleFilter(partCol,partiVals,partiType,Array(f))) {
               flagPart = true
             }
           }
           case Or(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) ||
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case And(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) &&
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case _ =>
         })
       } /*else if (partType.equals("BOOL")) {
//         val partBool = if (partiVal.toString.equals("0") || partiVal.toLowerCase.equals("false")) false else true
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
             if (!getDolphinDBPartitionBySingleFilter(partCol,Array(partiVal),partiType,Array(f))) {
               flagPart = true
             }
           }
           case Or(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, Array(partiVal),partiType, Array(f1)) ||
               getDolphinDBPartitionBySingleFilter(partCol, Array(partiVal),partiType, Array(f2))) {
               flagPart = true
             }
           }
           case And(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, Array(partiVal),partiType, Array(f1)) &&
               getDolphinDBPartitionBySingleFilter(partCol, Array(partiVal),partiType, Array(f2))) {
               flagPart = true
             }
           }
           case _ =>
         })
       }*/ else if (partType.equals("TIME")) {
         partFilter.foreach(f => f match {
           case EqualTo(attr, value) => {
             var typeFlag = false
             var userTime : String = null
             if (value.toString.contains(" ") || value.toString.contains("T")) { userTime = value.toString.split("[ T]")(1) }
             else userTime = value.toString
             val userTimeInt = Utils.countMilliseconds(LocalTime.parse(userTime))
             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partTimeInt = Utils.countMilliseconds(LocalTime.parse(pv.toString))
                 if (partTimeInt == userTimeInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partTimeInt0 = Utils.countMilliseconds(LocalTime.parse(partiVals(0).toString))
               val partTimeInt1 = Utils.countMilliseconds(LocalTime.parse(partiVals(1).toString))
               if (partTimeInt0 == partTimeInt1 && partTimeInt0 == userTimeInt) typeFlag = true
               else if ((partTimeInt0 <= userTimeInt && userTimeInt < partTimeInt1) ||
                            partTimeInt1 <= userTimeInt && userTimeInt < partTimeInt0) typeFlag = true
             } else {
               val partTimeInt = Utils.countMilliseconds(LocalTime.parse(partiVals(0)))
               if(partTimeInt == userTimeInt) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThan(attr, value) => {
             var typeFlag = false
             var userTime : String = null
             if (value.toString.contains(" ") || value.toString.contains("T")) { userTime = value.toString.split("[ T]")(1) }
             else userTime = value.toString
             val userTimeInt = Utils.countMilliseconds(LocalTime.parse(userTime))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partTimeInt = Utils.countMilliseconds(LocalTime.parse(pv.toString))
                 if (partTimeInt < userTimeInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partTimeInt0 = Utils.countMilliseconds(LocalTime.parse(partiVals(0).toString))
               val partTimeInt1 = Utils.countMilliseconds(LocalTime.parse(partiVals(1).toString))
               if ((partTimeInt0 < userTimeInt || partTimeInt1 < userTimeInt)) typeFlag = true
             } else {
               val partTimeInt = Utils.countMilliseconds(LocalTime.parse(partiVals(0)))
               if(partTimeInt < userTimeInt) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThan(attr, value) => {
             var userTime : String = null
             var typeFlag = false
             if (value.toString.contains(" ") || value.toString.contains("T")) { userTime = value.toString.split("[ T]")(1) }
             else userTime = value.toString
             val userTimeInt = Utils.countMilliseconds(LocalTime.parse(userTime))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partTimeInt = Utils.countMilliseconds(LocalTime.parse(pv.toString))
                 if (partTimeInt > userTimeInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partTimeInt0 = Utils.countMilliseconds(LocalTime.parse(partiVals(0).toString))
               val partTimeInt1 = Utils.countMilliseconds(LocalTime.parse(partiVals(1).toString))
               if ((partTimeInt0 > userTimeInt || partTimeInt1 > userTimeInt)) typeFlag = true
             } else {
               val partTimeInt = Utils.countMilliseconds(LocalTime.parse(partiVals(0)))
               if(partTimeInt > userTimeInt) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThanOrEqual(attr, value) => {
             var userTime : String = null
             var typeFlag = false
             if (value.toString.contains(" ") || value.toString.contains("T")) { userTime = value.toString.split("[ T]")(1) }
             else userTime = value.toString
             val userTimeInt = Utils.countMilliseconds(LocalTime.parse(userTime))
             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partTimeInt = Utils.countMilliseconds(LocalTime.parse(pv.toString))
                 if (partTimeInt <= userTimeInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partTimeInt0 = Utils.countMilliseconds(LocalTime.parse(partiVals(0).toString))
               val partTimeInt1 = Utils.countMilliseconds(LocalTime.parse(partiVals(1).toString))
               if ((partTimeInt0 <= userTimeInt || partTimeInt1 <= userTimeInt)) typeFlag = true
             } else {
               val partTimeInt = Utils.countMilliseconds(LocalTime.parse(partiVals(0)))
               if(partTimeInt <= userTimeInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThanOrEqual(attr, value) => {
             var userTime : String = null
             var typeFlag = false
             if (value.toString.contains(" ") || value.toString.contains("T")) { userTime = value.toString.split("[ T]")(1) }
             else userTime = value.toString
             val userTimeInt = Utils.countMilliseconds(LocalTime.parse(userTime))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partTimeInt = Utils.countMilliseconds(LocalTime.parse(pv.toString))
                 if (partTimeInt >= userTimeInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partTimeInt0 = Utils.countMilliseconds(LocalTime.parse(partiVals(0).toString))
               val partTimeInt1 = Utils.countMilliseconds(LocalTime.parse(partiVals(1).toString))
               if ((partTimeInt0 >= userTimeInt || partTimeInt1 >= userTimeInt)) typeFlag = true
             } else {
               val partTimeInt = Utils.countMilliseconds(LocalTime.parse(partiVals(0)))
               if(partTimeInt >= userTimeInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case In(attr, value) => {
             var typeFlag = false
             value.foreach(v => {
               var userTime : String = null
               if (v.toString.contains(" ") || v.toString.contains("T")) { userTime = v.toString.split("[ T]")(1) }
               else userTime = v.toString
               val userTimeInt = Utils.countMilliseconds(LocalTime.parse(userTime))

               if (partType == 3) {
                 partiVals.foreach(pv => {
                   val partTimeInt = Utils.countMilliseconds(LocalTime.parse(pv.toString))
                   if (partTimeInt == userTimeInt) typeFlag = true
                 })
               } else if (partType == 2) {
                 val partTimeInt0 = Utils.countMilliseconds(LocalTime.parse(partiVals(0).toString))
                 val partTimeInt1 = Utils.countMilliseconds(LocalTime.parse(partiVals(1).toString))
                 if (partTimeInt0 == partTimeInt1 && partTimeInt0 == userTimeInt) typeFlag = true
                 else if ((partTimeInt0 <= userTimeInt && userTimeInt < partTimeInt1) ||
                   partTimeInt1 <= userTimeInt && userTimeInt < partTimeInt0) typeFlag = true
               } else {
                 val partTimeInt = Utils.countMilliseconds(LocalTime.parse(partiVals(0)))
                 if(partTimeInt == userTimeInt) typeFlag = true
               }
             })
             if (attr.equalsIgnoreCase(partCol) && typeFlag) {
               flagPart = true
             }
           }
           case Not(f) => {
             if (!getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType,Array(f))) {
               flagPart = true
             }
           }
           case Or(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) ||
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case And(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) &&
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case _ =>
         })
       } else if (partType.equals("MINUTE")) {
         partFilter.foreach(f => f match {
           case EqualTo(attr, value) => {
             var typeFlag = false
             var userMin : String = null
             if (value.toString.contains(" ") || value.toString.contains("T")) { userMin = value.toString.split("[ T]")(1) }
             else userMin = value.toString
             val userMinInt = Utils.countMinutes(LocalTime.parse(userMin))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partMinInt = Utils.countMinutes(LocalTime.parse(pv.toString.replace("m", "")))
                 if (partMinInt == userMinInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partMinInt0 = Utils.countMinutes(LocalTime.parse(partiVals(0).toString.replace("m", "")))
               val partMinInt1 = Utils.countMinutes(LocalTime.parse(partiVals(1).toString.replace("m", "")))
               if (partMinInt0 == partMinInt1 && partMinInt0 == userMinInt) typeFlag = true
               else if ((partMinInt0 <= userMinInt && userMinInt < partMinInt1) ||
                 partMinInt1 <= userMinInt && userMinInt < partMinInt0) typeFlag = true
             } else {
               val partMinInt = Utils.countMinutes(LocalTime.parse(partiVals(0).toString.replace("m", "")))
               if(partMinInt == userMinInt) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThan(attr, value) => {
             var typeFlag = false
             var userMin : String = null
             if (value.toString.contains(" ") || value.toString.contains("T")) { userMin = value.toString.split("[ T]")(1) }
             else userMin = value.toString
             val userMinInt = Utils.countMinutes(LocalTime.parse(userMin))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partMinInt = Utils.countMinutes(LocalTime.parse(pv.toString.replace("m", "")))
                 if (partMinInt < userMinInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partMinInt0 = Utils.countMinutes(LocalTime.parse(partiVals(0).toString.replace("m", "")))
               val partMinInt1 = Utils.countMinutes(LocalTime.parse(partiVals(1).toString.replace("m", "")))
               if ((partMinInt0 < userMinInt || partMinInt1 < userMinInt)) typeFlag = true
             } else {
               val partMinInt = Utils.countMinutes(LocalTime.parse(partiVals(0).toString.replace("m", "")))
               if(partMinInt < userMinInt) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThan(attr, value) => {
             var typeFlag = false
             var userMin : String = null
             if (value.toString.contains(" ") || value.toString.contains("T")) { userMin = value.toString.split("[ T]")(1) }
             else userMin = value.toString
             val userMinInt = Utils.countMinutes(LocalTime.parse(userMin))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partMinInt = Utils.countMinutes(LocalTime.parse(pv.toString.replace("m", "")))
                 if (partMinInt > userMinInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partMinInt0 = Utils.countMinutes(LocalTime.parse(partiVals(0).toString.replace("m", "")))
               val partMinInt1 = Utils.countMinutes(LocalTime.parse(partiVals(1).toString.replace("m", "")))
               if ((partMinInt0 > userMinInt || partMinInt1 > userMinInt)) typeFlag = true
             } else {
               val partMinInt = Utils.countMinutes(LocalTime.parse(partiVals(0).toString.replace("m", "")))
               if(partMinInt > userMinInt) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThanOrEqual(attr, value) => {
             var typeFlag = false
             var userMin : String = null
             if (value.toString.contains(" ") || value.toString.contains("T")) { userMin = value.toString.split("[ T]")(1) }
             else userMin = value.toString
             val userMinInt = Utils.countMinutes(LocalTime.parse(userMin))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partMinInt = Utils.countMinutes(LocalTime.parse(pv.toString.replace("m", "")))
                 if (partMinInt <= userMinInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partMinInt0 = Utils.countMinutes(LocalTime.parse(partiVals(0).toString.replace("m", "")))
               val partMinInt1 = Utils.countMinutes(LocalTime.parse(partiVals(1).toString.replace("m", "")))
               if ((partMinInt0 <= userMinInt || partMinInt1 <= userMinInt)) typeFlag = true
             } else {
               val partMinInt = Utils.countMinutes(LocalTime.parse(partiVals(0).toString.replace("m", "")))
               if(partMinInt <= userMinInt) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThanOrEqual(attr, value) => {
             var typeFlag = false
             var userMin : String = null
             if (value.toString.contains(" ") || value.toString.contains("T")) { userMin = value.toString.split("[ T]")(1) }
             else userMin = value.toString
             val userMinInt = Utils.countMinutes(LocalTime.parse(userMin))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partMinInt = Utils.countMinutes(LocalTime.parse(pv.toString.replace("m", "")))
                 if (partMinInt >= userMinInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partMinInt0 = Utils.countMinutes(LocalTime.parse(partiVals(0).toString.replace("m", "")))
               val partMinInt1 = Utils.countMinutes(LocalTime.parse(partiVals(1).toString.replace("m", "")))
               if ((partMinInt0 >= userMinInt || partMinInt1 >= userMinInt)) typeFlag = true
             } else {
               val partMinInt = Utils.countMinutes(LocalTime.parse(partiVals(0).toString.replace("m", "")))
               if(partMinInt >= userMinInt) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case In(attr, value) => {
             var vin = false
             value.foreach(v => {
               var typeFlag = false
               var userMin : String = null
               if (v.toString.contains(" ") || v.toString.contains("T")) { userMin = v.toString.split("[ T]")(1) }
               else userMin = v.toString
               val userMinInt = Utils.countMinutes(LocalTime.parse(userMin))

               if (partType == 3) {
                 partiVals.foreach(pv => {
                   val partMinInt = Utils.countMinutes(LocalTime.parse(pv.toString.replace("m", "")))
                   if (partMinInt == userMinInt) typeFlag = true
                 })
               } else if (partType == 2) {
                 val partMinInt0 = Utils.countMinutes(LocalTime.parse(partiVals(0).toString.replace("m", "")))
                 val partMinInt1 = Utils.countMinutes(LocalTime.parse(partiVals(1).toString.replace("m", "")))
                 if (partMinInt0 == partMinInt1 && partMinInt0 == userMinInt) typeFlag = true
                 else if ((partMinInt0 <= userMinInt && userMinInt < partMinInt1) ||
                   partMinInt1 <= userMinInt && userMinInt < partMinInt0) typeFlag = true
               } else {
                 val partMinInt = Utils.countMinutes(LocalTime.parse(partiVals(0).toString.replace("m", "")))
                 if(partMinInt == userMinInt) typeFlag = true
               }
             })
             if (attr.equalsIgnoreCase(partCol) && vin) {
               flagPart = true
             }
           }
           case Not(f) => {
             if (!getDolphinDBPartitionBySingleFilter(partCol, partiVals, partiType,Array(f))) {
               flagPart = true
             }
           }
           case Or(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) ||
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case And(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) &&
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case _ =>
         })
       } else if (partType.equals("SECOND")) {
//         val partSecInt = Utils.countSeconds(LocalTime.parse(partiVal))
         partFilter.foreach(f => f match {
           case EqualTo(attr, value) => {
             var userSec : String = null
             var typeFlag = false
             if (value.toString.contains(" ") || value.toString.contains("T")) { userSec = value.toString.split("[ T]")(1) }
             else userSec = value.toString
             val userSecInt = Utils.countSeconds(LocalTime.parse(userSec))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partSecInt = Utils.countSeconds(LocalTime.parse(pv))
                 if (partSecInt == userSecInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partSecInt0 = Utils.countSeconds(LocalTime.parse(partiVals(0) ))
               val partSecInt1 = Utils.countSeconds(LocalTime.parse(partiVals(1) ))
               if (partSecInt0 == partSecInt1 && partSecInt0 == userSecInt) typeFlag = true
               else if ((partSecInt0 <= userSecInt && userSecInt < partSecInt1) ||
                 partSecInt1 <= userSecInt && userSecInt < partSecInt0) typeFlag = true
             } else {
               val partSecInt = Utils.countSeconds(LocalTime.parse(partiVals(0) ))
               if(partSecInt == userSecInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThan(attr, value) => {
             var typeFlag = false
             var userSec : String = null
             if (value.toString.contains(" ") || value.toString.contains("T")) { userSec = value.toString.split("[ T]")(1) }
             else userSec = value.toString
             val userSecInt = Utils.countSeconds(LocalTime.parse(userSec))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partSecInt = Utils.countSeconds(LocalTime.parse(pv))
                 if (partSecInt < userSecInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partSecInt0 = Utils.countSeconds(LocalTime.parse(partiVals(0) ))
               val partSecInt1 = Utils.countSeconds(LocalTime.parse(partiVals(1) ))
               if ((partSecInt0 < userSecInt && partSecInt1 < userSecInt) ) typeFlag = true
             } else {
               val partSecInt = Utils.countSeconds(LocalTime.parse(partiVals(0) ))
               if(partSecInt < userSecInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThan(attr, value) => {
             var typeFlag = false
             var userSec : String = null
             if (value.toString.contains(" ") || value.toString.contains("T")) { userSec = value.toString.split("[ T]")(1) }
             else userSec = value.toString
             val userSecInt = Utils.countSeconds(LocalTime.parse(userSec))
             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partSecInt = Utils.countSeconds(LocalTime.parse(pv))
                 if (partSecInt > userSecInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partSecInt0 = Utils.countSeconds(LocalTime.parse(partiVals(0) ))
               val partSecInt1 = Utils.countSeconds(LocalTime.parse(partiVals(1) ))
               if ((partSecInt0 > userSecInt && partSecInt1 > userSecInt) ) typeFlag = true
             } else {
               val partSecInt = Utils.countSeconds(LocalTime.parse(partiVals(0) ))
               if(partSecInt > userSecInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThanOrEqual(attr, value) => {
             var typeFlag = false
             var userSec : String = null
             if (value.toString.contains(" ") || value.toString.contains("T")) { userSec = value.toString.split("[ T]")(1) }
             else userSec = value.toString
             val userSecInt = Utils.countSeconds(LocalTime.parse(userSec))
             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partSecInt = Utils.countSeconds(LocalTime.parse(pv))
                 if (partSecInt <= userSecInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partSecInt0 = Utils.countSeconds(LocalTime.parse(partiVals(0) ))
               val partSecInt1 = Utils.countSeconds(LocalTime.parse(partiVals(1) ))
               if ((partSecInt0 <= userSecInt && partSecInt1 <= userSecInt) ) typeFlag = true
             } else {
               val partSecInt = Utils.countSeconds(LocalTime.parse(partiVals(0) ))
               if(partSecInt <= userSecInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThanOrEqual(attr, value) => {
             var typeFlag = false
             var userSec : String = null
             if (value.toString.contains(" ") || value.toString.contains("T")) { userSec = value.toString.split("[ T]")(1) }
             else userSec = value.toString
             val userSecInt = Utils.countSeconds(LocalTime.parse(userSec))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partSecInt = Utils.countSeconds(LocalTime.parse(pv))
                 if (partSecInt >= userSecInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partSecInt0 = Utils.countSeconds(LocalTime.parse(partiVals(0) ))
               val partSecInt1 = Utils.countSeconds(LocalTime.parse(partiVals(1) ))
               if ((partSecInt0 >= userSecInt && partSecInt1 >= userSecInt) ) typeFlag = true
             } else {
               val partSecInt = Utils.countSeconds(LocalTime.parse(partiVals(0) ))
               if(partSecInt >= userSecInt) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case In(attr, value) => {
             var typeFlag = false
             value.foreach(v => {
               var userSec : String = null
               if (v.toString.contains(" ") || v.toString.contains("T")) { userSec = v.toString.split("[ T]")(1) }
               else userSec = v.toString
               val userSecInt = Utils.countSeconds(LocalTime.parse(userSec))

               if (partType == 3) {
                 partiVals.foreach(pv => {
                   val partSecInt = Utils.countSeconds(LocalTime.parse(pv))
                   if (partSecInt == userSecInt) typeFlag = true
                 })
               } else if (partType == 2) {
                 val partSecInt0 = Utils.countSeconds(LocalTime.parse(partiVals(0) ))
                 val partSecInt1 = Utils.countSeconds(LocalTime.parse(partiVals(1) ))
                 if (partSecInt0 == partSecInt1 && partSecInt0 == userSecInt) typeFlag = true
                 else if ((partSecInt0 <= userSecInt && userSecInt < partSecInt1) ||
                   partSecInt1 <= userSecInt && userSecInt < partSecInt0) typeFlag = true
               } else {
                 val partSecInt = Utils.countSeconds(LocalTime.parse(partiVals(0) ))
                 if(partSecInt == userSecInt) typeFlag = true
               }
             })
             if (attr.equalsIgnoreCase(partCol) && typeFlag) {
               flagPart = true
             }
           }
           case Not(f) => {
             if (!getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType,Array(f))) {
               flagPart = true
             }
           }
           case Or(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol,partiVals,partiType, Array(f1)) ||
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case And(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) &&
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case _ =>
         })
       } else if (partType.equals("DATETIME")) {
         partFilter.foreach(f => f match {
           case EqualTo(attr, value) => {
             var typeFlag = false
             var userDT : String = null
             if (value.toString.contains(" ")) { userDT = value.toString.replace(" ", "T") }
             else userDT = value.toString
             val userDTInt = Utils.countSeconds(LocalDateTime.parse(userDT))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partDTInt = Utils.countSeconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                                      "T" + pv.split("[ T]")(1)))
                 if (partDTInt == userDTInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partDTInt0 = Utils.countSeconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                                      "T" + partiVals(0).split("[ T]")(1)))
               val partDTInt1 = Utils.countSeconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                                     "T" + partiVals(1).split("[ T]")(1)))
               if (partDTInt0 == partDTInt1 && partDTInt0 == userDTInt) typeFlag = true
               else if ((partDTInt0 <= userDTInt && userDTInt < partDTInt1) ||
                 partDTInt1 <= userDTInt && userDTInt < partDTInt0) typeFlag = true
             } else {
               val partDTInt = Utils.countSeconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                                      "T" + partiVals(0).split("[ T]")(1)))
               if(partDTInt == userDTInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag )) {
               flagPart = true
             }
           }
           case LessThan(attr, value) => {
             var typeFlag = false
             var userDT : String = null
             if (value.toString.contains(" ")) { userDT = value.toString.replace(" ", "T") }
             else userDT = value.toString
             val userDTInt = Utils.countSeconds(LocalDateTime.parse(userDT))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partDTInt = Utils.countSeconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                   "T" + pv.split("[ T]")(1)))
                 if (partDTInt < userDTInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partDTInt0 = Utils.countSeconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               val partDTInt1 = Utils.countSeconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(1).split("[ T]")(1)))
               if ((partDTInt0 < userDTInt || partDTInt1 < userDTInt) ) typeFlag = true
             } else {
               val partDTInt = Utils.countSeconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               if(partDTInt < userDTInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThan(attr, value) => {
             var typeFlag = false
             var userDT : String = null
             if (value.toString.contains(" ")) { userDT = value.toString.replace(" ", "T") }
             else userDT = value.toString
             val userDTInt = Utils.countSeconds(LocalDateTime.parse(userDT))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partDTInt = Utils.countSeconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                   "T" + pv.split("[ T]")(1)))
                 if (partDTInt > userDTInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partDTInt0 = Utils.countSeconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               val partDTInt1 = Utils.countSeconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(1).split("[ T]")(1)))
               if ((partDTInt0 > userDTInt || partDTInt1 > userDTInt) ) typeFlag = true
             } else {
               val partDTInt = Utils.countSeconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               if(partDTInt > userDTInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThanOrEqual(attr, value) => {
             var typeFlag = false
             var userDT : String = null
             if (value.toString.contains(" ")) { userDT = value.toString.replace(" ", "T") }
             else userDT = value.toString
             val userDTInt = Utils.countSeconds(LocalDateTime.parse(userDT))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partDTInt = Utils.countSeconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                   "T" + pv.split("[ T]")(1)))
                 if (partDTInt <= userDTInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partDTInt0 = Utils.countSeconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               val partDTInt1 = Utils.countSeconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(1).split("[ T]")(1)))
               if ((partDTInt0 <= userDTInt || partDTInt1 <= userDTInt) ) typeFlag = true
             } else {
               val partDTInt = Utils.countSeconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               if(partDTInt <= userDTInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThanOrEqual(attr, value) => {
             var typeFlag = false
             var userDT : String = null
             if (value.toString.contains(" ")) { userDT = value.toString.replace(" ", "T") }
             else userDT = value.toString
             val userDTInt = Utils.countSeconds(LocalDateTime.parse(userDT))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partDTInt = Utils.countSeconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                   "T" + pv.split("[ T]")(1)))
                 if (partDTInt >= userDTInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partDTInt0 = Utils.countSeconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               val partDTInt1 = Utils.countSeconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(1).split("[ T]")(1)))
               if ((partDTInt0 >= userDTInt || partDTInt1 >= userDTInt) ) typeFlag = true
             } else {
               val partDTInt = Utils.countSeconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               if(partDTInt >= userDTInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case In(attr, value) => {
             var typeFlag = false
             value.foreach(v => {
               var userDT : String = null
               if (v.toString.contains(" ")) { userDT = v.toString.replace(" ", "T") }
               else userDT = v.toString
               val userDTInt = Utils.countSeconds(LocalDateTime.parse(userDT))
               if (partType == 3) {
                 partiVals.foreach(pv => {
                   val partDTInt = Utils.countSeconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                     "T" + pv.split("[ T]")(1)))
                   if (partDTInt == userDTInt) typeFlag = true
                 })
               } else if (partType == 2) {
                 val partDTInt0 = Utils.countSeconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                   "T" + partiVals(0).split("[ T]")(1)))
                 val partDTInt1 = Utils.countSeconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                   "T" + partiVals(1).split("[ T]")(1)))
                 if (partDTInt0 == partDTInt1 && partDTInt0 == userDTInt) typeFlag = true
                 else if ((partDTInt0 <= userDTInt && userDTInt < partDTInt1) ||
                   partDTInt1 <= userDTInt && userDTInt < partDTInt0) typeFlag = true
               } else {
                 val partDTInt = Utils.countSeconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                   "T" + partiVals(0).split("[ T]")(1)))
                 if(partDTInt == userDTInt) typeFlag = true
               }
             })
             if (attr.equalsIgnoreCase(partCol) && typeFlag) {
               flagPart = true
             }
           }
           case Not(f) => {
             if (!getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType,Array(f))) {
               flagPart = true
             }
           }
           case Or(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) ||
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case And(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) &&
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case _ =>
         })
       } else if (partType.equals("TIMESTAMP")) {
//         val partTMInt = Utils.countMilliseconds(LocalDateTime.parse(partiVal.split("[ T]")(0).replace(".", "-")
//           + "T" + partiVal.split("[ T}")(1)))
         partFilter.foreach(f => f match {
           case EqualTo(attr, value) => {
             var typeFlag = false
             var userTM : String = null
             if (value.toString.contains(" ")) { userTM = value.toString.replace(" ", "T") }
             else userTM = value.toString
             val userTMInt = Utils.countMilliseconds(LocalDateTime.parse(userTM))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partTMInt = Utils.countMilliseconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                   "T" + pv.split("[ T]")(1)))
                 if (partTMInt == userTMInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partTMInt0 = Utils.countMilliseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               val partTMInt1 = Utils.countMilliseconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(1).split("[ T]")(1)))
               if (partTMInt0 == partTMInt1 && partTMInt0 == userTMInt) typeFlag = true
               else if ((partTMInt0 <= userTMInt && userTMInt < partTMInt1) ||
                 partTMInt1 <= userTMInt && userTMInt < partTMInt0) typeFlag = true
             } else {
               val partTMInt = Utils.countMilliseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               if(partTMInt == userTMInt) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThan(attr, value) => {
             var typeFlag = false
             var userTM : String = null
             if (value.toString.contains(" ")) { userTM = value.toString.replace(" ", "T") }
             else userTM = value.toString
             val userTMInt = Utils.countMilliseconds(LocalDateTime.parse(userTM))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partTMInt = Utils.countMilliseconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                   "T" + pv.split("[ T]")(1)))
                 if (partTMInt < userTMInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partTMInt0 = Utils.countMilliseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               val partTMInt1 = Utils.countMilliseconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(1).split("[ T]")(1)))
               if ((partTMInt0 < userTMInt || partTMInt1 < userTMInt) ) typeFlag = true
             } else {
               val partTMInt = Utils.countMilliseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               if(partTMInt < userTMInt) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThan(attr, value) => {
             var typeFlag = false
             var userTM : String = null
             if (value.toString.contains(" ")) { userTM = value.toString.replace(" ", "T") }
             else userTM = value.toString
             val userTMInt = Utils.countMilliseconds(LocalDateTime.parse(userTM))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partTMInt = Utils.countMilliseconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                   "T" + pv.split("[ T]")(1)))
                 if (partTMInt > userTMInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partTMInt0 = Utils.countMilliseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               val partTMInt1 = Utils.countMilliseconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(1).split("[ T]")(1)))
               if ((partTMInt0 > userTMInt || partTMInt1 > userTMInt) ) typeFlag = true
             } else {
               val partTMInt = Utils.countMilliseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               if(partTMInt > userTMInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThanOrEqual(attr, value) => {
             var typeFlag = false
             var userTM : String = null
             if (value.toString.contains(" ")) { userTM = value.toString.replace(" ", "T") }
             else userTM = value.toString
             val userTMInt = Utils.countMilliseconds(LocalDateTime.parse(userTM))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partTMInt = Utils.countMilliseconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                   "T" + pv.split("[ T]")(1)))
                 if (partTMInt <= userTMInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partTMInt0 = Utils.countMilliseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               val partTMInt1 = Utils.countMilliseconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(1).split("[ T]")(1)))
               if ((partTMInt0 <= userTMInt || partTMInt1 <= userTMInt) ) typeFlag = true
             } else {
               val partTMInt = Utils.countMilliseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               if(partTMInt <= userTMInt) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThanOrEqual(attr, value) => {
             var typeFlag = false
             var userTM : String = null
             if (value.toString.contains(" ")) { userTM = value.toString.replace(" ", "T") }
             else userTM = value.toString
             val userTMInt = Utils.countMilliseconds(LocalDateTime.parse(userTM))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partTMInt = Utils.countMilliseconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                   "T" + pv.split("[ T]")(1)))
                 if (partTMInt >= userTMInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partTMInt0 = Utils.countMilliseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               val partTMInt1 = Utils.countMilliseconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(1).split("[ T]")(1)))
               if ((partTMInt0 >= userTMInt || partTMInt1 >= userTMInt) ) typeFlag = true
             } else {
               val partTMInt = Utils.countMilliseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               if(partTMInt >= userTMInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag )) {
               flagPart = true
             }
           }
           case In(attr, value) => {
             var typeFlag = false
             value.foreach(v => {
               var userTM : String = null
               if (v.toString.contains(" ")) { userTM = v.toString.replace(" ", "T") }
               else userTM = v.toString
               val userTMInt = Utils.countMilliseconds(LocalDateTime.parse(userTM))

               if (partType == 3) {
                 partiVals.foreach(pv => {
                   val partTMInt = Utils.countMilliseconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                     "T" + pv.split("[ T]")(1)))
                   if (partTMInt == userTMInt) typeFlag = true
                 })
               } else if (partType == 2) {
                 val partTMInt0 = Utils.countMilliseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                   "T" + partiVals(0).split("[ T]")(1)))
                 val partTMInt1 = Utils.countMilliseconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                   "T" + partiVals(1).split("[ T]")(1)))
                 if (partTMInt0 == partTMInt1 && partTMInt0 == userTMInt) typeFlag = true
                 else if ((partTMInt0 <= userTMInt && userTMInt < partTMInt1) ||
                   partTMInt1 <= userTMInt && userTMInt < partTMInt0) typeFlag = true
               } else {
                 val partTMInt = Utils.countMilliseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                   "T" + partiVals(0).split("[ T]")(1)))
                 if(partTMInt == userTMInt) typeFlag = true
               }
             })
             if (attr.equalsIgnoreCase(partCol) && typeFlag) {
               flagPart = true
             }
           }
           case Not(f) => {
             if (!getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType,Array(f))) {
               flagPart = true
             }
           }
           case Or(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) ||
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case And(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) &&
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case _ =>
         })
       } else if (partType.equals("NANOTIME")) {
         partFilter.foreach(f => f match {
           case EqualTo(attr, value) => {
             var typeFlag = false
             var userNT : String = null
             if (value.toString.contains(" ")|| value.toString.contains("T")) { userNT = value.toString.split("[ T]")(1) }
             else userNT = value.toString
             val userNTLong = Utils.countNanoseconds(LocalTime.parse(userNT))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partNTLong = Utils.countNanoseconds(LocalTime.parse(pv))
                 if (partNTLong == userNTLong) typeFlag = true
               })
             } else if (partType == 2) {
               val partNTLong0 = Utils.countNanoseconds(LocalTime.parse(partiVals(0)))
               val partNTLong1 = Utils.countNanoseconds(LocalTime.parse(partiVals(1)))
               if (partNTLong0 == partNTLong1 && partNTLong0 == userNTLong) typeFlag = true
               else if ((partNTLong0 <= userNTLong && userNTLong < partNTLong1) ||
                 partNTLong1 <= userNTLong && userNTLong < partNTLong0) typeFlag = true
             } else {
               val partTMInt = Utils.countNanoseconds(LocalTime.parse(partiVals(0)))
               if(partTMInt == userNTLong) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThan(attr, value) => {
             var typeFlag = false
             var userNT : String = null
             if (value.toString.contains(" ")|| value.toString.contains("T")) { userNT = value.toString.split("[ T]")(1) }
             else userNT = value.toString
             val userNTLong = Utils.countNanoseconds(LocalTime.parse(userNT))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partNTLong = Utils.countNanoseconds(LocalTime.parse(pv))
                 if (partNTLong < userNTLong) typeFlag = true
               })
             } else if (partType == 2) {
               val partNTLong0 = Utils.countNanoseconds(LocalTime.parse(partiVals(0)))
               val partNTLong1 = Utils.countNanoseconds(LocalTime.parse(partiVals(1)))
               if (partNTLong0 == partNTLong1 && partNTLong0 == userNTLong) typeFlag = true
               else if ((partNTLong0 < userNTLong || partNTLong1 < userNTLong)) typeFlag = true
             } else {
               val partTMInt = Utils.countNanoseconds(LocalTime.parse(partiVals(0)))
               if(partTMInt < userNTLong) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThan(attr, value) => {
             var typeFlag = false
             var userNT : String = null
             if (value.toString.contains(" ")|| value.toString.contains("T")) { userNT = value.toString.split("[ T]")(1) }
             else userNT = value.toString
             val userNTLong = Utils.countNanoseconds(LocalTime.parse(userNT))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partNTLong = Utils.countNanoseconds(LocalTime.parse(pv))
                 if (partNTLong > userNTLong) typeFlag = true
               })
             } else if (partType == 2) {
               val partNTLong0 = Utils.countNanoseconds(LocalTime.parse(partiVals(0)))
               val partNTLong1 = Utils.countNanoseconds(LocalTime.parse(partiVals(1)))
               if (partNTLong0 == partNTLong1 && partNTLong0 == userNTLong) typeFlag = true
               else if ((partNTLong0 > userNTLong || partNTLong1 > userNTLong)) typeFlag = true
             } else {
               val partTMInt = Utils.countNanoseconds(LocalTime.parse(partiVals(0)))
               if(partTMInt > userNTLong) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThanOrEqual(attr, value) => {
             var typeFlag = false
             var userNT : String = null
             if (value.toString.contains(" ")|| value.toString.contains("T")) { userNT = value.toString.split("[ T]")(1) }
             else userNT = value.toString
             val userNTLong = Utils.countNanoseconds(LocalTime.parse(userNT))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partNTLong = Utils.countNanoseconds(LocalTime.parse(pv))
                 if (partNTLong <= userNTLong) typeFlag = true
               })
             } else if (partType == 2) {
               val partNTLong0 = Utils.countNanoseconds(LocalTime.parse(partiVals(0)))
               val partNTLong1 = Utils.countNanoseconds(LocalTime.parse(partiVals(1)))
               if (partNTLong0 == partNTLong1 && partNTLong0 == userNTLong) typeFlag = true
               else if ((partNTLong0 <= userNTLong || partNTLong1 <= userNTLong)) typeFlag = true
             } else {
               val partTMInt = Utils.countNanoseconds(LocalTime.parse(partiVals(0)))
               if(partTMInt <= userNTLong) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThanOrEqual(attr, value) => {
             var typeFlag = false
             var userNT : String = null
             if (value.toString.contains(" ")|| value.toString.contains("T")) { userNT = value.toString.split("[ T]")(1) }
             else userNT = value.toString
             val userNTLong = Utils.countNanoseconds(LocalTime.parse(userNT))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partNTLong = Utils.countNanoseconds(LocalTime.parse(pv))
                 if (partNTLong >= userNTLong) typeFlag = true
               })
             } else if (partType == 2) {
               val partNTLong0 = Utils.countNanoseconds(LocalTime.parse(partiVals(0)))
               val partNTLong1 = Utils.countNanoseconds(LocalTime.parse(partiVals(1)))
               if (partNTLong0 == partNTLong1 && partNTLong0 == userNTLong) typeFlag = true
               else if ((partNTLong0 >= userNTLong || partNTLong1 >= userNTLong)) typeFlag = true
             } else {
               val partTMInt = Utils.countNanoseconds(LocalTime.parse(partiVals(0)))
               if(partTMInt >= userNTLong) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case In(attr, value) => {
             var typeFlag = false
             value.foreach(v => {
               var userNT : String = null
               if (v.toString.contains(" ") || value.toString.contains("T")) { userNT = v.toString.split("[ T]")(1) }
               else userNT = v.toString
               val userNTLong = Utils.countMilliseconds(LocalDateTime.parse(userNT))
               if (partType == 3) {
                 partiVals.foreach(pv => {
                   val partNTLong = Utils.countNanoseconds(LocalTime.parse(pv))
                   if (partNTLong == userNTLong) typeFlag = true
                 })
               } else if (partType == 2) {
                 val partNTLong0 = Utils.countNanoseconds(LocalTime.parse(partiVals(0)))
                 val partNTLong1 = Utils.countNanoseconds(LocalTime.parse(partiVals(1)))
                 if (partNTLong0 == partNTLong1 && partNTLong0 == userNTLong) typeFlag = true
                 else if ((partNTLong0 <= userNTLong && userNTLong < partNTLong1) ||
                   partNTLong1 <= userNTLong && userNTLong < partNTLong0) typeFlag = true
               } else {
                 val partTMInt = Utils.countNanoseconds(LocalTime.parse(partiVals(0)))
                 if(partTMInt == userNTLong) typeFlag = true
               }
             })
             if (attr.equalsIgnoreCase(partCol) && typeFlag) {
               flagPart = true
             }
           }
           case Not(f) => {
             if (!getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType,Array(f))) {
               flagPart = true
             }
           }
           case Or(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) ||
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case And(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) &&
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case _ =>
         })
       } else if (partType.equals("NANOTIMESTAMP")) {
         partFilter.foreach(f => f match {
           case EqualTo(attr, value) => {
             var typeFlag = false
             var userNTS : String = null
             if (value.toString.contains(" ")) { userNTS = value.toString.replace(" ", "T") }
             else userNTS = value.toString
             val userNTSLong = Utils.countNanoseconds(LocalDateTime.parse(userNTS))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partNTSLong = Utils.countNanoseconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                   "T" + pv.split("[ T]")(1)))
                 if (partNTSLong == userNTSLong) typeFlag = true
               })
             } else if (partType == 2) {
               val partNTSLong0 = Utils.countNanoseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               val partNTSLong1 = Utils.countNanoseconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(1).split("[ T]")(1)))
               if (partNTSLong0 == partNTSLong1 && partNTSLong0 == userNTSLong) typeFlag = true
               else if ((partNTSLong0 <= userNTSLong && userNTSLong < partNTSLong1) ||
                 partNTSLong1 <= userNTSLong && userNTSLong < partNTSLong0) typeFlag = true
             } else {
               val partNTSLong = Utils.countNanoseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               if(partNTSLong == userNTSLong) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThan(attr, value) => {
             var typeFlag = false
             var userNTS : String = null
             if (value.toString.contains(" ")) { userNTS = value.toString.replace(" ", "T") }
             else userNTS = value.toString
             val userNTSLong = Utils.countNanoseconds(LocalDateTime.parse(userNTS))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partNTSLong = Utils.countNanoseconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                   "T" + pv.split("[ T]")(1)))
                 if (partNTSLong < userNTSLong) typeFlag = true
               })
             } else if (partType == 2) {
               val partNTSLong0 = Utils.countNanoseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               val partNTSLong1 = Utils.countNanoseconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(1).split("[ T]")(1)))
               if ((partNTSLong0 < userNTSLong || partNTSLong1 < userNTSLong) ) typeFlag = true
             } else {
               val partNTSLong = Utils.countNanoseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               if(partNTSLong < userNTSLong) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThan(attr, value) => {
             var typeFlag = false
             var userNTS : String = null
             if (value.toString.contains(" ")) { userNTS = value.toString.replace(" ", "T") }
             else userNTS = value.toString
             val userNTSLong = Utils.countNanoseconds(LocalDateTime.parse(userNTS))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partNTSLong = Utils.countNanoseconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                   "T" + pv.split("[ T]")(1)))
                 if (partNTSLong > userNTSLong) typeFlag = true
               })
             } else if (partType == 2) {
               val partNTSLong0 = Utils.countNanoseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               val partNTSLong1 = Utils.countNanoseconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(1).split("[ T]")(1)))
               if ((partNTSLong0 > userNTSLong || partNTSLong1 > userNTSLong) ) typeFlag = true
             } else {
               val partNTSLong = Utils.countNanoseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               if(partNTSLong > userNTSLong) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThanOrEqual(attr, value) => {
             var typeFlag = false
             var userNTS : String = null
             if (value.toString.contains(" ")) { userNTS = value.toString.replace(" ", "T") }
             else userNTS = value.toString
             val userNTSLong = Utils.countNanoseconds(LocalDateTime.parse(userNTS))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partNTSLong = Utils.countNanoseconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                   "T" + pv.split("[ T]")(1)))
                 if (partNTSLong <= userNTSLong) typeFlag = true
               })
             } else if (partType == 2) {
               val partNTSLong0 = Utils.countNanoseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               val partNTSLong1 = Utils.countNanoseconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(1).split("[ T]")(1)))
               if ((partNTSLong0 <= userNTSLong || partNTSLong1 <= userNTSLong) ) typeFlag = true
             } else {
               val partNTSLong = Utils.countNanoseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               if(partNTSLong <= userNTSLong) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThanOrEqual(attr, value) => {
             var typeFlag = false
             var userNTS : String = null
             if (value.toString.contains(" ")) { userNTS = value.toString.replace(" ", "T") }
             else userNTS = value.toString
             val userNTSLong = Utils.countNanoseconds(LocalDateTime.parse(userNTS))

             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partNTSLong = Utils.countNanoseconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                   "T" + pv.split("[ T]")(1)))
                 if (partNTSLong >= userNTSLong) typeFlag = true
               })
             } else if (partType == 2) {
               val partNTSLong0 = Utils.countNanoseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               val partNTSLong1 = Utils.countNanoseconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(1).split("[ T]")(1)))
               if ((partNTSLong0 >= userNTSLong || partNTSLong1 >= userNTSLong) ) typeFlag = true
             } else {
               val partNTSLong = Utils.countNanoseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                 "T" + partiVals(0).split("[ T]")(1)))
               if(partNTSLong >= userNTSLong) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case In(attr, value) => {
             var typeFlag = false
             value.foreach(v => {
               var userNTS : String = null
               if (v.toString.contains(" ")) { userNTS = v.toString.replace(" ", "T") }
               else userNTS = v.toString
               val userNTSLong = Utils.countNanoseconds(LocalDateTime.parse(userNTS))

               if (partType == 3) {
                 partiVals.foreach(pv => {
                   val partNTSLong = Utils.countNanoseconds(LocalDateTime.parse(pv.split("[ T]")(0).replace(".", "-") +
                     "T" + pv.split("[ T]")(1)))
                   if (partNTSLong == userNTSLong) typeFlag = true
                 })
               } else if (partType == 2) {
                 val partNTSLong0 = Utils.countNanoseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                   "T" + partiVals(0).split("[ T]")(1)))
                 val partNTSLong1 = Utils.countNanoseconds(LocalDateTime.parse(partiVals(1).split("[ T]")(0).replace(".", "-") +
                   "T" + partiVals(1).split("[ T]")(1)))
                 if (partNTSLong0 == partNTSLong1 && partNTSLong0 == userNTSLong) typeFlag = true
                 else if ((partNTSLong0 <= userNTSLong && userNTSLong < partNTSLong1) ||
                   partNTSLong1 <= userNTSLong && userNTSLong < partNTSLong0) typeFlag = true
               } else {
                 val partNTSLong = Utils.countNanoseconds(LocalDateTime.parse(partiVals(0).split("[ T]")(0).replace(".", "-") +
                   "T" + partiVals(0).split("[ T]")(1)))
                 if(partNTSLong == userNTSLong) typeFlag = true
               }
             })
             if (attr.equalsIgnoreCase(partCol) && typeFlag) {
               flagPart = true
             }
           }
           case Not(f) => {
             if (!getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType,Array(f))) {
               flagPart = true
             }
           }
           case Or(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) ||
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case And(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) &&
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case _ =>
         })
       } else if (partType.equals("DATE")) {
         partFilter.foreach(f => f match {
           case EqualTo(attr, value) => {
            var typeFlag = false
             val userDInt = Utils.countDays(LocalDate.parse(value.toString))
             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partDInt = Utils.countDays(LocalDate.parse(pv.replace(".", "-")))
                 if (partDInt == userDInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partDInt0 = Utils.countDays(LocalDate.parse(partiVals(0).replace(".", "-")))
               val partDInt1 = Utils.countDays(LocalDate.parse(partiVals(1).replace(".", "-")))
               if (partDInt0 == partDInt1 && partDInt0 == userDInt) typeFlag = true
               else if ((partDInt0 <= userDInt && userDInt < partDInt1) ||
                 partDInt1 <= userDInt && userDInt < partDInt0) typeFlag = true
             } else {
               val partDInt = Utils.countDays(LocalDate.parse(partiVals(0).replace(".", "-")))
               if(partDInt == userDInt) typeFlag = true
             }
             if (attr.equalsIgnoreCase(partCol) && typeFlag) {
               flagPart = true
             }
           }
           case LessThan(attr, value) => {
             var typeFlag = false
             val userDInt = Utils.countDays(LocalDate.parse(value.toString))
             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partDInt = Utils.countDays(LocalDate.parse(pv.replace(".", "-")))
                 if (partDInt < userDInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partDInt0 = Utils.countDays(LocalDate.parse(partiVals(0).replace(".", "-")))
               val partDInt1 = Utils.countDays(LocalDate.parse(partiVals(1).replace(".", "-")))
               if ((partDInt0 < userDInt || partDInt1 < userDInt)) typeFlag = true
             } else {
               val partDInt = Utils.countDays(LocalDate.parse(partiVals(0).replace(".", "-")))
               if(partDInt < userDInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThan(attr, value) => {
             var typeFlag = false
             val userDInt = Utils.countDays(LocalDate.parse(value.toString))
             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partDInt = Utils.countDays(LocalDate.parse(pv.replace(".", "-")))
                 if (partDInt > userDInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partDInt0 = Utils.countDays(LocalDate.parse(partiVals(0).replace(".", "-")))
               val partDInt1 = Utils.countDays(LocalDate.parse(partiVals(1).replace(".", "-")))
               if ((partDInt0 > userDInt || partDInt1 > userDInt)) typeFlag = true
             } else {
               val partDInt = Utils.countDays(LocalDate.parse(partiVals(0).replace(".", "-")))
               if(partDInt > userDInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case LessThanOrEqual(attr, value) => {

             var typeFlag = false
             val userDInt = Utils.countDays(LocalDate.parse(value.toString))
             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partDInt = Utils.countDays(LocalDate.parse(pv.replace(".", "-")))
                 if (partDInt <= userDInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partDInt0 = Utils.countDays(LocalDate.parse(partiVals(0).replace(".", "-")))
               val partDInt1 = Utils.countDays(LocalDate.parse(partiVals(1).replace(".", "-")))
               if ((partDInt0 <= userDInt || partDInt1 <= userDInt)) typeFlag = true
             } else {
               val partDInt = Utils.countDays(LocalDate.parse(partiVals(0).replace(".", "-")))
               if(partDInt <= userDInt) typeFlag = true
             }
             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case GreaterThanOrEqual(attr, value) => {

             var typeFlag = false
             val userDInt = Utils.countDays(LocalDate.parse(value.toString))
             if (partType == 3) {
               partiVals.foreach(pv => {
                 val partDInt = Utils.countDays(LocalDate.parse(pv.replace(".", "-")))
                 if (partDInt >= userDInt) typeFlag = true
               })
             } else if (partType == 2) {
               val partDInt0 = Utils.countDays(LocalDate.parse(partiVals(0).replace(".", "-")))
               val partDInt1 = Utils.countDays(LocalDate.parse(partiVals(1).replace(".", "-")))
               if ((partDInt0 >= userDInt || partDInt1 >= userDInt)) typeFlag = true
             } else {
               val partDInt = Utils.countDays(LocalDate.parse(partiVals(0).replace(".", "-")))
               if(partDInt >= userDInt) typeFlag = true
             }

             if ((attr.equalsIgnoreCase(partCol) && typeFlag)) {
               flagPart = true
             }
           }
           case In(attr, value) => {
             var vin = false
             value.foreach(v => {

               var typeFlag = false
               val userDInt = Utils.countDays(LocalDate.parse(value.toString))
               if (partType == 3) {
                 partiVals.foreach(pv => {
                   val partDInt = Utils.countDays(LocalDate.parse(pv.replace(".", "-")))
                   if (partDInt == userDInt) typeFlag = true
                 })
               } else if (partType == 2) {
                 val partDInt0 = Utils.countDays(LocalDate.parse(partiVals(0).replace(".", "-")))
                 val partDInt1 = Utils.countDays(LocalDate.parse(partiVals(1).replace(".", "-")))
                 if (partDInt0 == partDInt1 && partDInt0 == userDInt) typeFlag = true
                 else if ((partDInt0 <= userDInt && userDInt < partDInt1) ||
                   partDInt1 <= userDInt && userDInt < partDInt0) typeFlag = true
               } else {
                 val partDInt = Utils.countDays(LocalDate.parse(partiVals(0).replace(".", "-")))
                 if(partDInt == userDInt) typeFlag = true
               }
               if (attr.equalsIgnoreCase(partCol) && typeFlag) {
                 flagPart = true
               }
             })
           }
           case Not(f) => {
             if (!getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType,Array(f))) {
               flagPart = true
             }
           }
           case Or(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) ||
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case And(f1, f2) => {
             if (getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f1)) &&
               getDolphinDBPartitionBySingleFilter(partCol, partiVals,partiType, Array(f2))) {
               flagPart = true
             }
           }
           case _ =>
         })
       } else {
         flagPart = false
       }
//     }
     flagPart
  }


}
