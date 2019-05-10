package com.dolphindb.spark

import java.time.{LocalDate, LocalDateTime, LocalTime}

import com.dolphindb.spark.exception.NoDataBaseException
import com.dolphindb.spark.schema.DolphinDBOptions
import com.xxdb.DBConnection
import com.xxdb.data.{BasicTable, Utils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

object DolphinDBUtils extends Logging{

  /**
    * Number of writes into DolphinDB per time
    */
  val DolphinDB_PER_NUM = 30000

  /**
    * DolphinDB Table partition Type num
    */
  val DolphinDB_Partition_SEQ = 0
  val DolphinDB_Partition_VALUE = 1
  val DolphinDB_Partition_RANGE = 2
  val DolphinDB_Partition_LIST = 3
  val DolphinDB_Partition_COMPO = 4
  val DolphinDB_Partition_HASH = 5

  /**
    * Parses the user specified customSchema option value to DataFrame schema, and
    * returns a schema that is replaced by the custom schema's dataType if column name is matched.
    */
  def getCustomSchema(
     tableSchema: StructType,
     customSchema: String,
     nameEquality: Resolver): StructType = {

    if (null != customSchema && customSchema.nonEmpty){
      val userSchema = CatalystSqlParser.parseTableSchema(customSchema)

      val newSchema = tableSchema.map(tbl => {
        userSchema.find(cus => tbl.name.equalsIgnoreCase(cus.name)) match {
          case Some(c) => tbl.copy(dataType = c.dataType)
          case None => tbl
        }
      })
      StructType(newSchema)
    } else {
      tableSchema
    }
  }


  /**
    *  Create a DolphinDB Connection
    * @param options
    * @return DBConnection
    */
  def createDolphinDBConn(options : DolphinDBOptions): DBConnection = {
    val ip = options.ip.get
    val port = options.port.get.toInt
    val user = options.user.get
    val password = options.password.get
    val conn = new DBConnection()
    conn.connect(ip, port, user, password)
    conn
  }


  /**
    * Judgment DolphinDB table exists
    * @param conn
    * @param dBOptions
    */
  def tableExists(conn :DBConnection, dBOptions: DolphinDBOptions) = {
    val dbPath = dBOptions.dbPath
    val table = dBOptions.table

    if (conn.run(s"existsDatabase('$dbPath')") == null) {
      logError(s"No Database ${dbPath}")
      throw new NoDataBaseException(s"No DataBase : ${dbPath}")
    }
    try {
      conn.run(s"${table} = database('$dbPath').loadTable('${table}')")
      true
    } catch {
      case e : java.io.IOException => false
    }

  }

  /**
    * Gets the DolphinDB table origin schema
    * @param conn
    * @param dBOptions
    * @return
    */
  def getDolphinDBSchema(conn: DBConnection, dBOptions: DolphinDBOptions) : Array[(String, String)] = {
    val table = dBOptions.table
    val schemaTB = conn.run(s"schema(${table}).colDefs").asInstanceOf[BasicTable]
    val dolphinDBName2Type = new ArrayBuffer[(String, String)]()
    for (i <- 0 until(schemaTB.rows())) {
      dolphinDBName2Type += (schemaTB.getColumn(0).get(i).toString ->
        schemaTB.getColumn(1).get(i).getString)
    }
    dolphinDBName2Type.toArray
  }


  /**
    * case DolphinDB type return value after compute
    * @return
    */
  def getLongValue (colType : String, colVal : String) : Long = colType match {
    case "DATE" =>
      Utils.countDays(LocalDate.parse(colVal.replace(".", "-")))
    case "MONTH" =>
      val monthStr = colVal.replace("M", "").split("\\.")
      Utils.countMonths(monthStr(0).toInt, monthStr(1).toInt)
    case "TIME" =>
      Utils.countMilliseconds(LocalTime.parse(colVal))
    case "MINUTE" =>
      val minute = colVal.replace("m", "")
      Utils.countMinutes(minute.split(":")(0).toInt, minute.split(":")(1).toInt)
    case "SECOND" =>
      Utils.countSeconds(LocalTime.parse(colVal))
    case "DATETIME" =>
      var datetimestr = colVal.replace(".", "-")
      if (datetimestr.contains(" ")) {
        datetimestr = datetimestr.replace(" ", "T")
      }
      Utils.countSeconds(LocalDateTime.parse(datetimestr))
    case "TIMESTAMP" =>
      val colValTmp = if (colVal.contains(" ")) colVal.replace(" ", "T") else colVal
      val colArr = colValTmp.split("T")
      Utils.countMilliseconds(LocalDateTime.parse(colArr(0).replace(".", "-") + "T" + colArr(1)))
    case "NANOTIME" =>
      Utils.countNanoseconds(LocalTime.parse(colVal))
    case "NANOTIMESTAMP" =>
      val colValTmp = if (colVal.contains(" ")) colVal.replace(" ", "T") else colVal
      val colArr = colValTmp.split("T")
      Utils.countMilliseconds(LocalDateTime.parse(colArr(0).replace(".", "-") + "T" + colArr(1)))
    case "VOID" =>
      0
    case "BOOL" =>
      if ((!java.lang.Boolean.parseBoolean(colVal)) || 0 == colVal.toInt) 0.toByte else 1.toByte
    case "LONG" => colVal.toLong
    case "INT" => colVal.toLong
    case "SHORT" => colVal.toLong
    case "CHAR" => colVal.charAt(0).toByte
    case _ => 0
  }

  /**
    * case DolphinDB type return value after compute
    * @param colType
    * @param colVal
    * @return
    */
  def getDoubleValue (colType : String, colVal : String) : Double = colType match {
    case "DOUBLE" => colVal.toDouble
    case "FLOAT" => colVal.toFloat
    case _ => 0.0
  }
}
