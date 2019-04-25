package com.dolphindb.spark.partition

import com.dolphindb.spark.rdd.DolphinDBRDD
import com.dolphindb.spark.schema.{DolphinDBOptions, DolphinDBSchema}
import com.xxdb.DBConnection
import com.xxdb.data.{BasicAnyVector, Vector}
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
//    var partiVal : Vector = null
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
      // There are multiple partitions in DolphinDB table
    if (partiVals.isInstanceOf[BasicAnyVector] && partiCols.length > 1) {
      var partiValArr = new ArrayBuffer[ArrayBuffer[String]]()
      val partiVector = partiVals.asInstanceOf[BasicAnyVector]

      // There are not partition field in User-defined SQL
      if (partiFilters.length == 0) {
        for (i <- 0 until(partiVector.rows())) {
          val tmpPartiVal = new ArrayBuffer[ArrayBuffer[String]]()
          val vector = partiVector.getEntity(i).asInstanceOf[Vector]
          for (j <- 0 until(vector.rows())) {
            // i = 0 means the first level partition
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
      // There are partition fields in User-defined SQL
      else {

      }

    }
    //  There is only one partition in DolphinDB table.
    else {
      // There are not partition field in User-defined SQL
      if (partiFilters.length == 0){
        for (pi <- 0 until(partiVals.rows())) {
          partitions += new DolphinDBPartition(pi, addrs, partiCols, Array(partiVals.get(pi).toString))
        }
      }
      // There are partition fields in User-defined SQL
      else {
        val partiValArr = new ArrayBuffer[ArrayBuffer[String]]()
//        for (pi <- 0 until(partiVals.rows())) {
          getDolphinDBPartitionBySingleFilter(partiValArr, partiCols(0), partiVals, partiFilters.toArray)

//        }
      }
    }

    /**
      * 此处要处理filter 与  dolphindb 中的分区字段匹配， 不是所有的都要处理，
      * 如果用户的查询条件就是有分区字段，就是不用去扫描所有的分区字段。
      */

    partitions.toArray
  }


  private def getDolphinDBPartitionBySingleFilter(
         partiValArr: ArrayBuffer[ArrayBuffer[String]],
         partCol :String, partiVals : Vector,
         partFilter: Array[Filter]): Unit = {

    // Get the partiton column type in dolphindb
     DolphinDBRDD.originNameToType.get(partCol).get match {
       case "SYMBOL" =>
         for (i <- 0 until(partiVals.rows())) {

           partFilter.foreach(f => f match {
             case EqualTo(attr, value) =>{
               if (attr.equalsIgnoreCase(partCol) &&
                 partiVals.get(i).toString.equals(value.toString)) {
                    return
               }
             }
//             case



           })
           partiValArr += ArrayBuffer[String](partiVals.get(i).toString)
         }
       case "INT" =>
     }



  }



  private def getDolphinDBPartitionBySingleFilter(f : Filter,
         addrs: mutable.HashMap[String, ArrayBuffer[Int]],
         partiCols : Array[String],
         partiVals: Vector,
       partitions: ArrayBuffer[DolphinDBPartition]): Unit = {
    f match {
      case EqualTo(attr, value) =>
        partitions += new DolphinDBPartition(0, addrs, partiCols, Array[String](value.toString))
      case LessThan(attr, value) =>
//      val partType = DolphinDBRDD.originNameToType.get(attr)

        for(i <- 0 until(partiVals.rows())) {
          if (partiVals.get(i).toString < ){

          }
        }
    }
  }


}
