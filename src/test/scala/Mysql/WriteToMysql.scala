package Mysql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object WriteToMysql {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()

    val spark = SparkSession.builder().appName("WriteToMysql")
      .master("local[2]")
      .getOrCreate()

    val dframe = spark.read.format("com.dolphindb.spark.DolphinDBProvider")
      .option("ip", "115.239.209.224")
      .option("port", 16961)
      .option("user", "admin")
      .option("password", "123456")
      //      .option("tbPath" , "dfs://chao/TAQ/db/taq")
      .option("tbPath" , "dfs://chao/Spark/db/sparktb1")
      .load()

    dframe.createTempView("sd1")

    val frame = spark.sql( s"""select BIDSIZ ,SYMBOL , time from sd1 where
         | Symbol='A' and
         |  date = to_date('2007-08-16')
         | """.stripMargin)

  println(frame.collect().length)
    frame.write.mode("append").jdbc("jdbc:mysql://115.239.209.189:3306/spark-conn?user=root&password=123456", "taq", new Properties())



    val endTime = System.currentTimeMillis()
    println("=====ALL TIME====" + (endTime - startTime))
    println("============================")
    spark.stop()
  }

}
