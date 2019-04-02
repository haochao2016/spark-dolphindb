package com.dolphindb.spark

import com.dolphindb.spark.schema.DolphinDBOptions
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.{JDBC_LOWER_BOUND, JDBC_UPPER_BOUND}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class DolphinDBProvider extends RelationProvider
//                            with SchemaRelationProvider
                            with DataSourceRegister {

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
//  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = ???
}
