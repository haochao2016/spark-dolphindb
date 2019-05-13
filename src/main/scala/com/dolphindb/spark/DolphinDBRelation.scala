package com.dolphindb.spark

import com.dolphindb.spark.partition.{ DolphinDBPartitioner}
import com.dolphindb.spark.rdd.DolphinDBRDD
import com.dolphindb.spark.schema.DolphinDBOptions
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

private [spark] object DolphinDBRelation extends Logging {

  /**
    * Given a partitioning schematic (a column of integral type, a number of
    * partitions, on the column's value), generate
    * WHERE clauses for each partition so that each row in the table appears
    * exactly once.  The parameters minValue and maxValue are advisory in that
    * incorrect values may cause the partitioning to be poor, but no data
    * will fail to be represented.
    *
    * Null value predicate is added to the first partition where clause to include
    * the rows with null value for the partitions column.
    *
    * @param partitioning partition information to generate the where clause for each partition
    * @return an array of partitions with where clause for each partition
    */
 /* def columnPartition(partitioning: DolphinDBPartitioningInfo): Array[Partition] = {
    if (partitioning==null || partitioning.numPartitions <=1 ) {
      return Array[Partition](DolphinDBPartition(null, 0))
    }

    val partitions = new ArrayBuffer[Partition]()
    for (i <- 0 until(partitioning.numPartitions)) {

    }
    return Array[Partition](DolphinDBPartition(null, 0))

  }*/
}


case class DolphinDBRelation (
//           parts : Array[Partition],
           options : DolphinDBOptions
             /*parameters: Map[String, String],
              schemaProvided: Option[StructType] = None*/ )(
            @transient val sparkSession: SparkSession )
              extends BaseRelation
              with PrunedFilteredScan
              with InsertableRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = {
    val tableSchema = DolphinDBRDD.resolveTable(options)
    options.customSchema match {
      case Some(customSchema) => DolphinDBUtils.getCustomSchema(
        tableSchema, customSchema, sparkSession.sessionState.conf.resolver)
      case None => tableSchema
    }
  }

  private val dolphindbPartis = new DolphinDBPartitioner(options)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val partis = dolphindbPartis.computGenericPartition(filters)

    DolphinDBRDD.scanTable(sparkSession.sparkContext,
      schema, requiredColumns, filters, dolphindbPartis.computGenericPartition(filters), options).asInstanceOf[RDD[Row]]
  }

  /**
    * Override insert method to insert data into this dataframe
    * @param data DataFrame of input
    * @param overwrite Whether the table is overwriten
    */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    /**
      * Get the DataFrame schema
      */
    val schema = data.schema
    if (overwrite) {

    }

    data.foreachPartition(dataPart => {
//      new DolphinDBWriter(options).save(dataPart, schema)
    })

  }
}
