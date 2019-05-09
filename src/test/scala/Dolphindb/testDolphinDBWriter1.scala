package Dolphindb

import org.apache.spark.sql.{SaveMode, SparkSession}

object testDolphinDBWriter1 {


  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()

    val spark = SparkSession.builder().appName("testDolphinDBWriter1")
      .master("local[2]")
      .getOrCreate()

    val sd = spark.read.format("com.dolphindb.spark.DolphinDBProvider")
      .option("ip", "192.168.1.13")
      .option("port", 16961)
      .option("user", "admin")
      .option("password", "123456")
      .option("tbPath" , "dfs://chao/Spark/db/sparktb1")
      .load()

    sd.printSchema()
    sd.createTempView("sd1")

    val frame = spark.sql(
      s"""select * from sd1
         | """.stripMargin)
    frame.printSchema()
    println("==============frame===============")



//    println(frame.collect().length)
    frame.show()


    frame.write.format("com.dolphindb.spark.DolphinDBProvider")
      .option("ip", "192.168.1.13")
      .option("port", 16961)
      .option("user", "admin")
      .option("password", "123456")
      .option("tbPath" , "dfs://chao/Spark/db/sparktb1")
      .mode(SaveMode.Append)
      .save()
//
//
//    val endTime = System.currentTimeMillis()
//    println("=====ALL TIME====" + (endTime - startTime))
//    spark.stop()

    //    frame.write.format("com.dolphindb.spark.DolphinDBProvider").options()


  }
}
