package com.dolphindb.spark.schema

import java.util.{Locale, Properties}

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class DolphinDBOptions (@transient private val parameters: CaseInsensitiveMap[String])
                    extends Serializable {
  import DolphinDBOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  def this(ip: String, port: String, tbPath: String, user : String,
           password : String, parameters: Map[String, String]) = {
    this(CaseInsensitiveMap(parameters ++ Map(
      DolphinDBOptions.DolphinDB_DBTB -> tbPath,
      DolphinDBOptions.DolphinDB_IP -> ip,
      DolphinDBOptions.DolphinDB_PORT -> port,
      DolphinDBOptions.DolphinDB_USER -> user,
      DolphinDBOptions.DolphinDB_PASSWORD -> password )))
  }

  /**
    * return a property with all options.
    */
  val asProperties : Properties = {
    val properties = new Properties()
    parameters.originalMap.foreach{ case (k, v) => properties.setProperty(k ,v)  }
    properties
  }

  /**
    * Returns a property with all options except Spark Internal data source options like "tbPath",
    *  and "numPartition". This should be used whern invoking DolphinDB API like "DBConnection"
    *  because data in DolphinDB
    */
  val asConnectionProperties: Properties = {
    val properties = new Properties()
    parameters.originalMap.filterKeys(key => !dolphinDBOptionNames(key.toLowerCase(Locale.ROOT)))
      .foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  require(parameters.isDefinedAt(DolphinDB_DBTB), s"Option '$DolphinDB_DBTB' is required.")
  require(parameters.isDefinedAt(DolphinDB_IP), s"Option '$DolphinDB_IP' is required.")
  require(parameters.isDefinedAt(DolphinDB_PORT), s"Option '$DolphinDB_PORT' is required.")
  require(parameters.isDefinedAt(DolphinDB_USER), s"Option '$DolphinDB_USER' is required.")
  require(parameters.isDefinedAt(DolphinDB_PASSWORD), s"Option '$DolphinDB_PASSWORD' is required.")


  /******************************************************/
  /*** Required parameters **/
  /******************************************************/
  /**
    * DolphinDB dataNode database path and table path
    *  Used in at any DataNode
    */
  val tbPath = parameters.get(DolphinDB_DBTB)
  val dbPath = tbPath.get.substring(0, tbPath.get.lastIndexOf("/"))
  val table = tbPath.get.substring(tbPath.get.lastIndexOf("/") + 1)
  /**
    * DolphinDB dataNode IP
    */
  val ip = parameters.get(DolphinDB_IP)
  /**
    * DolphinDB dataNode Port
    */
  val port = parameters.get(DolphinDB_PORT)
  /**
    * DolphinDB dataNode user
    */
  val user = parameters.get(DolphinDB_USER)
  /**
    * DolphinDB dataNode password
    */
  val password = parameters.get(DolphinDB_PASSWORD)

  /******************************************************/
  /*** Optional parameters **/
  /******************************************************/

  /**
    *  the number of partitions
    */
  val numPartitions = parameters.get(DolphinDB_NUM_PARTITIONS).map(_.toInt)


  /******************************************************/
  /*** Optional parameters only for reading **/
  /******************************************************/
  /**
    * The Column used to partition
    */
  val partitionColumn = parameters.get(DolphinDB_PARTITION_COLUMN)

//     the lower bound of partition column
//    val lowerBound = parameters.get(DolphinDB_LOWER_BOUND).map(_.toLong)
//     the upper bound of the partition column
//    val upperBound = parameters.get(DolphinDB_UPPER_BOUND).map(_.toLong)

  require((partitionColumn.isEmpty || partitionColumn.isDefined),
      s"When reading DolphinDB data sources, users need to specify all or none for the following " +
      s"options: '$DolphinDB_PARTITION_COLUMN' ")

  val fetchSize = {
    val size = parameters.getOrElse(DolphinDB_BATCH_FETCH_SIZE, "0").toInt
    require(size >= 0,
       s"Invalid value `${size.toString}` for parameter " +
        s"`$DolphinDB_BATCH_FETCH_SIZE`. The minimum value is 0. When the value is 0, " +
        "the DolphinDB driver ignores the value and does the estimates.")
    size
  }

  /******************************************************/
  /***  Optional parameters only for writing   **/
  /******************************************************/

  /**
    * truncate table from the DolphinDB database
    */
  val isTruncate = parameters.getOrElse(DolphinDB_TRUNCATE, "false").toBoolean
  /** the create table option , which can be table_options or partition_options.
  //  * E.g., "CREATE TABLE t (name string) ENGINE=InnoDB DEFAULT CHARSET=utf8"
   * TODO: to reuse the existing partition parameters for those partition specific options
    * */
 // val createTableOptions = parameters.getOrElse(DolphinDB_CREATE_TABLE_OPTIONS, "")
  val createTableColumnTypes = parameters.get(DolphinDB_CREATE_TABLE_COLUMN_TYPES)
  val customSchema = parameters.get(DolphinDB_CUSTOM_DATAFRAME_COLUMN_TYPES)

  /* DolphinDB write all data, not batch  */
  /*val batchSize = {
    val size = parameters.getOrElse(JDBC_BATCH_INSERT_SIZE, "1000").toInt
    require(size >= 1,
      s"Invalid value `${size.toString}` for parameter " +
        s"`$JDBC_BATCH_INSERT_SIZE`. The minimum value is 1.")
    size
  }*/


  /* JDBC isolationLevel does not apply to DolphinDB */
 /* val isolationLevel =
    parameters.getOrElse(JDBC_TXN_ISOLATION_LEVEL, "READ_UNCOMMITTED") match {
      case "NONE" => Connection.TRANSACTION_NONE
      case "READ_UNCOMMITTED" => Connection.TRANSACTION_READ_UNCOMMITTED
      case "READ_COMMITTED" => Connection.TRANSACTION_READ_COMMITTED
      case "REPEATABLE_READ" => Connection.TRANSACTION_REPEATABLE_READ
      case "SERIALIZABLE" => Connection.TRANSACTION_SERIALIZABLE
    }*/


  //  An option to execute custom SQL before fetching data from the remote DolphinDB
  val sessionInitStatement = parameters.get(DolphinDB_SESSION_INIT_STATEMENT)

}


object DolphinDBOptions {

  private val dolphinDBOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String) : String = {
    dolphinDBOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  /**  example :
    * {{{
    *   "dfs://db1/tb1,dfs://db1/tb2,dfs://db2/tb1"
    * }}}
    */
  val DolphinDB_DBTB = newOption("tbPath")
//  val DolphinDB_TABLE = newOption("table")
  val DolphinDB_IP = newOption("ip")
  val DolphinDB_PORT = newOption("port")
  val DolphinDB_USER = newOption("user")
  val DolphinDB_PASSWORD = newOption("password")

  val DolphinDB_NUM_PARTITIONS = newOption("numPartitions")
  val DolphinDB_PARTITION_COLUMN = newOption("partitionColumnName")

  val DolphinDB_BATCH_FETCH_SIZE = newOption("fetchSize")

  val DolphinDB_TRUNCATE = newOption("truncate")
  val DolphinDB_CREATE_TABLE_COLUMN_TYPES = newOption("createTableColumnTypes")
  val DolphinDB_CUSTOM_DATAFRAME_COLUMN_TYPES = newOption("customSchema")

  val DolphinDB_SESSION_INIT_STATEMENT = newOption("sessionInitStatement")

}