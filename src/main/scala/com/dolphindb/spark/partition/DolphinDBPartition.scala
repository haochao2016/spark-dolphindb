package com.dolphindb.spark.partition

import org.apache.spark.Partition

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class  DolphinDBPartition(var index : Int,
                               hosts : mutable.HashMap[String, ArrayBuffer[Int]],
                               partiCols : Array[String],
                               partiVals : Array[String]) extends Partition

