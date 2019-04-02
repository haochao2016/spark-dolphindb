package com.dolphindb.spark.schema

import java.sql.{Date, Timestamp}

import com.dolphindb.spark.rdd.DolphinDBRDD
import org.apache.commons.lang3.StringUtils
import org.apache.spark.annotation.Since

class DolphinDBDialects extends Serializable {

  /**
    * Quotes the identifier. This is used to put quotes around the identifier in case the column
    * name is a reserved keyword, or in case it contains characters that require quotes (e.g. space).
    */
  def quoteIdentifier(colName: String): String = {
    s""""$colName""""
  }


  /**
    * Escape special characters in SQL string literals.
    * @param value The string to be escaped.
    * @return Escaped string.
    */
  protected[spark] def escapeSql(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "''")

  /**
    * Converts value to SQL expression.
    * @param value The value to be converted.
    * @return Converted value.
    */
  def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case dateValue: Date => "'" + dateValue + "'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }

//  def matchType (colName) : String = match {
//      DolphinDBRDD.schemaOrigin
//
//  }
}

object DolphinDBDialects {
  def apply: DolphinDBDialects = new DolphinDBDialects()
}
