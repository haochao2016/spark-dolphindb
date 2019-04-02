package com.dolphindb.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.StructType

object DolphinDBUtils extends Logging{

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


}
