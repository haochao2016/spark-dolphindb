package com.dolphindb.spark.rdd


import java.net.InetAddress

import com.dolphindb.spark.exception.NoDataBaseException
import com.dolphindb.spark.partition.DolphinDBPartition
import com.dolphindb.spark.schema.{DolphinDBDialects, DolphinDBOptions, DolphinDBSchema}
import com.xxdb.DBConnection
import com.xxdb.data.BasicTable
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Data corresponding to one partition of a DolphinDBRDD.
  * @param whereClause
  * @param idx
  */
//case class DolphinDBPartition(whereClause: String, idx: Int) extends Partition {
//  override def index: Int = idx
//}

object DolphinDBRDD extends Logging {

  /**
    * DolphinDB table size
    * the size is table columns queried
    */
//  var tblColSize = 0
  /**
    * DolphinDB table schema in Spark
    */
  var schemaOrigin : StructType= null
  /**
    *  originType contains All type of table ordering in DolphinDB
    */
  val originType = new ArrayBuffer[String]()
  /**
    *  DolphinDB table colName -> colType in DolphinDB
    */
  val originNameToType = new mutable.HashMap[String, String]

  /**
    * Takes a (schema, table) specification and returns the table's Catalyst
    * schema.
    *
    * @param options - DolphinDB options that contains ip, port, user,
    *                password, tbPath and other information.
    *
    * @return A StructType giving the table's Catalyst schema.
    */
    def resolveTable(options : DolphinDBOptions) : StructType = {
      val tbPath = options.tbPath.get
      val ip = options.ip.get
      val port = options.port.get.toInt
      val user = options.user.get
      val password = options.password.get
      /**
        * create a DolphinDB Connection
        */
      val conn = new DBConnection()
      conn.connect(ip, port, user, password)

      /**
        * ALL DolphinDB_Path & DolphinDB_Table
        *  An example :
        *  {{{
        *   [("dfs://dolphin/db", "tb1"),
        *   ("dfs://dolphin/db", "tb2")]
        *  }}}
        * */
      val tbPathTupArr = {
        val tbPathArr = tbPath.split(",")
//        if (sql.equals("") && tbPathArr.length != 1 ) {
//          throw new TableException("There can only be one table without specifying SQL ")
//        } else {

          val tbPathTupArrTmp = new ArrayBuffer[(String, String)]()
          tbPathArr.foreach{ case s /*: String*/ =>
            val dbPath = s.substring(0, s.lastIndexOf("/"))
            val tbName = s.substring(s.lastIndexOf("/") + 1)
            val db_path_table = (dbPath, tbName)
            tbPathTupArrTmp += db_path_table
          }
          tbPathTupArrTmp
//        }
      }

      /**
        * load DolphinDB Table
        */
      tbPathTupArr.foreach{ case(db, tb) => {
          if (conn.run(s"existsDatabase('${db}')") == null) {
            logError(s"No Database '${db}'")
            throw new NoDataBaseException(s"No DataBase : '${db}'")
          }
          val dbhandle = db.substring(db.lastIndexOf("/") +1) + tb
          conn.run(s"${dbhandle}=database('${db}'); ${tb}=${dbhandle}.loadTable('${tb}')")
        }
      }

    /**
      *  Get Schema
      *  There are two situations.
      *  Users have input a DolphinDB SQL or Not
      */
    var schemaDB : BasicTable = null

    /**
      *  Array contains tuple (name, type)
      * An example :
      * {{{
      *  [(id : String),
      *   (name : String)]
      *   }}}
      * */
      val StructArr = new ArrayBuffer[StructField]()

      //Only one dfs path
      if (tbPathTupArr.length == 1) {
        logInfo(s"Get the ${tbPathTupArr(0)._2} Schema ")
        schemaDB = conn.run(s"schema(${tbPathTupArr(0)._2}).colDefs").asInstanceOf[BasicTable]
//        tblColSize = schemaDB.rows()
        for (i <- 0 until(schemaDB.rows())) {
          val struct = DolphinDBSchema.convertToStructField(schemaDB.getColumn(0).get(i).getString,
            schemaDB.getColumn(1).get(i).getString)
          originType += schemaDB.getColumn(1).get(i).getString
          originNameToType += (schemaDB.getColumn(0).get(i).getString ->
            schemaDB.getColumn(1).get(i).getString)
          StructArr += struct
        }
      } else {

      }
      schemaOrigin = StructType(StructArr)
      schemaOrigin
    }

  /**
    *  Prune all but the specified columns from the specified Catalyst schema.
    * @param schema The Catalyst schema of the master table
    * @param columns The list of desired columns
    * @return A Catalyst schema corresponding to columns in the given order.
    */
  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name =>  fieldMap(name)))
  }

  def compilerFilter(f : Filter) : Option[String] ={

    def typeCol (colName : String, value : Any) : String = {
     val oriType = DolphinDBRDD.originNameToType.get(colName).get
//      oriType is a type in DolphinDB
      oriType match {
        case "SYMBOL" => s""""${value.toString}""""
        case "STRING" => s""""${value.toString}""""
        case "DATE" => {
          if (value.toString.contains("-")) value.toString.replace("-", ".")
          else value.toString
        }
        case "MONTH" => {
          if (value.toString.contains("-")) { value.toString.substring(0, value.toString.lastIndexOf("-"))
            .replace("-", ".") + "M" }
          else value.toString
        }
        case "TIME" => {
          val timeval = value.toString.split(" ")(1)
          if( timeval.contains(".") && timeval.split(".")(1).length == 1) {
            timeval.split(".")(0)
          } else timeval
        }
        case "MINUTE" => {
          val minute = value.toString.split(" ")(1)
          minute.substring(0, minute.lastIndexOf(":")) + "m"
        }
        case "SECOND" => value.toString.split(" ")(1).split(".")(0)
        case "DATETIME" => value.toString.split(".")(0)
        case "TIMESTAMP" => value.toString
        case "NANOTIME" => value.toString.split(" ")(1)
        case "NANOTIMESTAMP" => value.toString
        case "CHAR" => value.toString.charAt(0).toByte.toString
        case _ => value.toString
      }
    }

    Option( f match {
      case EqualTo(attr, value) => s" ${attr} =  ${typeCol(attr, value)} "
      case LessThan(attr, value) => s" ${attr} < ${typeCol(attr, value)} "
      case GreaterThan(attr, value) => s" ${attr} > ${typeCol(attr, value)} "
      case LessThanOrEqual(attr, value) => s" ${attr} <= ${typeCol(attr, value)} "
      case GreaterThanOrEqual(attr, value) => s" ${attr} >= ${typeCol(attr, value)} "
      case IsNull(attr) => s" isNull(${attr}) "
      case IsNotNull(attr) => s" not(isNull(${attr})) "
      case StringStartsWith(attr, value) => s" ${attr} like '${value}%' "
      case StringEndsWith(attr, value) => s" ${attr} like '%${value}' "
      case StringContains(attr, value) => s" ${attr} like '%${value}%' "
      case In(attr, value) =>  s"""${attr} in (${value.map(v => typeCol(attr, v)).mkString(",")})"""
      case Not(f) => compilerFilter(f).map(p => s" not($p) ").get
      case Or(f1, f2) =>
        val orf = Seq(f1, f2).flatMap(compilerFilter(_))
        orf.map(p => s" (${p}) ").mkString(" or ")
      case And(f1, f2) =>
        val andf = Seq(f1, f2).flatMap(compilerFilter(_))
        andf.map(p => s"${p}").mkString(" and ")
      case _ => null
     }
    )

  }


  def scanTable(
         sc :SparkContext,
         schema : StructType,
         requiredColumns: Array[String],
         filters : Array[Filter],
         parts: Array[Partition],
         options: DolphinDBOptions
         ): RDD[Row] = {

    val ip = options.ip.get
    val port = options.port.get.toInt
    val user = options.user.get
    val password = options.password.get
    val dialects = new DolphinDBDialects()
    val quotedColumns = requiredColumns//.map( dialects.quoteIdentifier(_))

    new DolphinDBRDD(sc,
      ip, port, user, password,
      pruneSchema(schema, requiredColumns),
      quotedColumns,
      filters, parts, options)

  }

}

/**
  * An RDD representing a table in a database accessed via DolphinDB.  Both the
  * driver code and the workers must be able to access the database; the driver
  * needs to fetch the schema while the workers need to fetch the data.
  */
private[spark] class DolphinDBRDD(
           sc: SparkContext,
           ip : String, port : Int, user : String, password : String,
           schema : StructType,
           columns: Array[String],
           filters : Array[Filter],
           partitions: Array[Partition],
           options: DolphinDBOptions)
   extends RDD[Row](sc, Nil) {

//  InternalRow
//  columns.foreach(println)
//  println("===========filter=================")
//  filters.foreach(println(_))
//  println("===========filter=================")

  /**
    * Retrieve the list of partitions corresponding to this RDD.
    */
  override protected def getPartitions: Array[Partition] = partitions

  /**
    * `columns`, but as a String suitable for injection into a SQL query.
    */
  private val columnList : String ={
    val builder = new StringBuilder()
    columns.foreach(builder.append(",").append(_))
    if (builder.isEmpty) "1" else builder.substring(1)
  }

  /**
    * A WHERE clause representing both `filters`, if any, and the current partition.
    */
  private val filterWhereClause : String = {
    val filter = filters.flatMap(DolphinDBRDD.compilerFilter(_))
      .map(x => s"$x").mkString(" and ")
    filter
  }

  /**
    * Get query conditions for DolphinDB
    * @param part  DolphinDBPartition
    * @return
    */
  private def getWhereClause(part : DolphinDBPartition) :String = {
    val partCondition = new mutable.StringBuilder("")
    /**  Add partition condition    */
    if (part.partiCols != null) {
      for (i <- 0 until(part.partiCols.length)) {
        val colType = DolphinDBRDD.originNameToType.get(part.partiCols(i)).get.toUpperCase()
        partCondition.append(part.partiCols(i))

        if (part.partiVals(i).length > 1) {
          if (part.partiTypes(i) == 2) {    /**  DolphinDB partititon type is range  */
            if (colType.equals("STRING") || colType.equals("SYMBOL")) {
              if (part.partiVals(i)(0).equals(part.partiVals(i)(1))) partCondition.append(" = \"" + part.partiVals(i)(0) + "\"")
              else {
                /** If reverse range partitioning is used here ,
                  * The data type must be determined
                  * There is no determination of the data type  */
                if (part.partiVals(i)(0) < part.partiVals(i)(1)){
                  partCondition.append(" >= \"" + part.partiVals(i)(0) + "\"")
                  partCondition.append(" and "+ part.partiCols(i) +" < \"" + part.partiVals(i)(1) + "\"")
                } else {
                  partCondition.append(" >= \"" + part.partiVals(i)(1) + "\"")
                  partCondition.append(" and "+ part.partiCols(i) +"< \"" + part.partiVals(i)(0) + "\"")
                }
              }
            } else {
              if (part.partiVals(i)(0) == part.partiVals(i)(1)) partCondition.append(" = " + part.partiVals(i)(0))
              else {
                if (part.partiVals(i)(0) < part.partiVals(i)(1)){
                  partCondition.append(" >= " + part.partiVals(i)(0))
                  partCondition.append(" and "+ part.partiCols(i) +"< " + part.partiVals(i)(1))
                } else {
                  partCondition.append(" >= " + part.partiVals(i)(1))
                  partCondition.append(" and "+ part.partiCols(i) +"< " + part.partiVals(i)(0))
                }
              }
            }
          } else if (part.partiTypes(i) == 3) {   /**  DolphinDB partititon type is List    */
            partCondition.append(" in ")
            val partCIB = new mutable.StringBuilder("( ")
            for (partCI <- part.partiVals(i)){
              if (colType.equals("STRING") || colType.equals("SYMBOL")) partCIB.append( "\""+ partCI + "\"" + ",")
              else partCIB.append(partCI + ",")
            }
            partCondition.append(partCIB.substring(0, partCIB.length - 1) + ")")
          }
        } else {
          partCondition.append(" = ")
          if (colType.equals("STRING") || colType.equals("SYMBOL")) partCondition.append( "\""+ part.partiVals(i)(0) + "\"")
          else partCondition.append(part.partiVals(i)(0))
        }
        partCondition.append(" and ")
      }
    }
    if (filterWhereClause.length > 0)  " where " + partCondition.append(s" $filterWhereClause ").toString()
    else if (partCondition.length > 0) " where " + partCondition.substring(0, partCondition.lastIndexOf("and"))
    else ""
  }

  /**
    * Runs the SQL query on DolphinDB Java-api driver.
    */
  override def compute(parts: Partition, context: TaskContext): Iterator[Row] = {
    var closed = false
    var conn = new DBConnection

    def close(): Unit = {
      if (closed) return
      if (null != conn) {
        conn.close()
        logInfo("closed connection")
      }
      closed = true
    }
    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener(context => close())

    val inputMetrics = context.taskMetrics().inputMetrics

    val part = parts.asInstanceOf[DolphinDBPartition]
    val hosts = part.hosts
    val hostAddress = InetAddress.getLocalHost.getHostAddress
    if (part.partiCols == null|| part.partiCols.length == 0) {
      conn.connect(ip, port,user, password)
    } else if (part.hosts.contains(hostAddress)) {
      val ports = part.hosts.get(hostAddress).get
      conn.connect(hostAddress, ports(Random.nextInt(ports.size)) ,user, password)
    } else {
      val keyArr = part.hosts.keySet.toArray
      val newHost = keyArr(Random.nextInt(keyArr.length))
      if (newHost.contains(ip.substring(0, ip.lastIndexOf(".")))) {
        /**  n the same network segment    */
        val ports = part.hosts.get(newHost).get
        conn.connect(newHost, ports(Random.nextInt(ports.size)) ,user, password)
      } else {
        conn.connect(ip, port,user, password)
      }
    }

    /**
      * load Table
      */
    if (conn.run(s"existsDatabase('${options.dbPath}')") == null) {
      logError(s"No Database ${options.dbPath}")
      throw new NoDataBaseException(s"No DataBase : ${options.dbPath}")
    }
    val dbhandle = options.dbPath.substring(options.dbPath.lastIndexOf("/") +1) + options.table
    conn.run(s"${dbhandle}=database('${options.dbPath}'); ${options.table}=${dbhandle}.loadTable('${options.table}')")

    val myWhereClause = getWhereClause(part)

    val sqlText = s"select  $columnList from ${options.table} ${myWhereClause} "
    logInfo(s"SQL :   ${sqlText}")
    val dolphinDBTable = conn.run(sqlText).asInstanceOf[BasicTable]

    new DolphinDBRDDIterator(
      context,
      parts,
      options,
      schema,
      dolphinDBTable
    )

  }

}

