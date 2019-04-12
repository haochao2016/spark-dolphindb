package com.dolphindb.spark

import com.dolphindb.spark.rdd.DolphinDBRDD
import com.dolphindb.spark.schema.DolphinDBOptions
import com.dolphindb.spark.writer.DolphinDBWriter
import com.xxdb.DBConnection
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.{JDBC_LOWER_BOUND, JDBC_UPPER_BOUND}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

class DolphinDBProvider extends RelationProvider
//                            with SchemaRelationProvider
                            with DataSourceRegister
                            with CreatableRelationProvider{

  override def shortName(): String = "DolphinDB"


  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    import com.dolphindb.spark.schema.DolphinDBOptions._

    val dolphinDBOptions = new DolphinDBOptions(parameters)
    val partitionColumn = dolphinDBOptions.partitionColumn
    val numPartitions = dolphinDBOptions.numPartitions

    val partitionInfo = if (partitionColumn.isEmpty) {
      assert(partitionColumn.isEmpty, "When 'partitionColumn' is not specified, " +
        s"'$DolphinDB_NUM_PARTITIONS' are expected to be empty")
      null
    } else {
      assert(numPartitions.nonEmpty,  s"When 'partitionColumn' is specified, '$DolphinDB_PARTITION_COLUMN' are also required")
      DolphinDBPartitioningInfo(partitionColumn.get, numPartitions.get)
    }
    val parts = DolphinDBRelation.columnPartition(partitionInfo)

    DolphinDBRelation(parts , dolphinDBOptions)(sqlContext.sparkSession)
  }

//  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
//                              schema : StructType): BaseRelation = {
//
//    DolphinDBRelation(parameters, Option(schema))(sqlContext.sparkSession)
//  }


  /**
    * Save a DataFrame to DolphinDB table
    * @param sqlContext
    * @param mode
    * @param parameters
    * @param data
    * @return
    */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {

    val dBOptions = new DolphinDBOptions(parameters)
    val conn = DolphinDBUtils.createDolphinDBConn(dBOptions)
    val dataSchema = data.schema

    /**
      *  Judgment DolphinDB table exists
      */
    val tableExists = DolphinDBUtils.tableExists(conn, dBOptions)
    if (tableExists) {
      val name2Type = DolphinDBUtils.getDolphinDBSchema(conn, dBOptions)

      /**
        * 判断 dataSchema 与 name2Type 在名字与类型上是否匹配
        * 如果不匹配 return
        */


      mode match  {
        case SaveMode.Overwrite => {

        }
        case SaveMode.Append => {
          data.foreachPartition( it => {
            new DolphinDBWriter(dBOptions).save(conn, it, name2Type)
          })
        }
        case SaveMode.ErrorIfExists => {
//          throw new AnalysisException(
//            s"DolphinDB Table '${dBOptions.table}' already exists. SaveMode: ErrorIfExists.")
        }
        case SaveMode.Ignore =>
      }


    }





    /**
      * case mode
      */





    data.foreachPartition( it => {
//      new DolphinDBWriter(new DolphinDBOptions(parameters)).save(it, )

    })




    createRelation(sqlContext, parameters)
  }
}
