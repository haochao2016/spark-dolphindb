package com.dolphindb.spark

import com.dolphindb.spark.exception.{NoTableException, SparkTypeMismatchDolphinDBTypeException}
import com.dolphindb.spark.schema.{DolphinDBOptions, DolphinDBSchema}
import com.dolphindb.spark.writer.DolphinDBWriter
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources._

class DolphinDBProvider extends RelationProvider
//                            with SchemaRelationProvider
                            with DataSourceRegister
                            with CreatableRelationProvider{

  override def shortName(): String = "DolphinDB"


  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    import com.dolphindb.spark.schema.DolphinDBOptions._

    val dolphinDBOptions = new DolphinDBOptions(parameters)

//    val partitionColumn = dolphinDBOptions.partitionColumn
//    val numPartitions = dolphinDBOptions.numPartitions
//
//    val partitionInfo = if (partitionColumn.isEmpty) {
//      assert(partitionColumn.isEmpty, "When 'partitionColumn' is not specified, " +
//        s"'$DolphinDB_NUM_PARTITIONS' are expected to be empty")
//      null
//    } else {
//      assert(numPartitions.nonEmpty,  s"When 'partitionColumn' is specified, '$DolphinDB_PARTITION_COLUMN' are also required")
//      DolphinDBPartitioningInfo(partitionColumn.get, numPartitions.get)
//    }
//    val parts = DolphinDBRelation.columnPartition(partitionInfo)

    DolphinDBRelation(dolphinDBOptions)(sqlContext.sparkSession)
  }

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
    val dataFields = data.schema.fields
    /**
      *  Judgment DolphinDB table exists
      */
    val tableExists = DolphinDBUtils.tableExists(conn, dBOptions)
    if (tableExists) {
      val name2Type = DolphinDBUtils.getDolphinDBSchema(conn, dBOptions)
      conn.close()

      /**
        * Judge whether Spark dataFrame dataType and DolphinDB dataType match.
        * if not match throw a Exception
        */
      for (i <- 0 until(name2Type.length)) {
        if (dataFields(i).name.toLowerCase.equals(name2Type(i)._1.toLowerCase())) {
          val matchFlag = dataFields(i).dataType.typeName.equals(DolphinDBSchema
              .convertToStructField(name2Type(i)._1, name2Type(i)._2).dataType.typeName)
          if (!matchFlag) throw new SparkTypeMismatchDolphinDBTypeException("Written data types do not match those in the DolphinDB database")
        } else {
          throw new SparkTypeMismatchDolphinDBTypeException("Written data types do not match those in the DolphinDB database")
        }
      }

      mode match  {
        case SaveMode.Overwrite =>
        case SaveMode.Append => {
          data.foreachPartition( it => {
            new DolphinDBWriter(dBOptions).save(it, name2Type)
          })
        }
        case SaveMode.ErrorIfExists =>
        case SaveMode.Ignore =>
      }

    } else {
      throw new NoTableException(s"No table ${dBOptions.table} in database ${dBOptions.dbPath}")
    }
    createRelation(sqlContext, parameters)
  }
}
