package com.dolphindb.spark.writer

import com.dolphindb.spark.schema.DolphinDBOptions
import com.xxdb.DBConnection
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

class DolphinDBWriter(options: DolphinDBOptions) extends Logging{

  protected val conn = new DBConnection
  private val ip = options.ip.get
  private val port = options.port.get.toInt
  private val user = options.user.get
  private val password = options.password.get




  def save(it: Iterator[Row], schema: StructType): Unit = {

  }

}
