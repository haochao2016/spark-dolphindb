package Hive

import org.apache.spark.sql.SparkSession

object testHiveData {


  def main(args: Array[String]): Unit = {

    val beginTime = System.currentTimeMillis()
    println("    =======起始时间=====   " + beginTime)


    val spark = SparkSession.builder().appName("HIVE_DATA")
      //      .master("local[2]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://chao/hive/warehouse")
      .getOrCreate();


    val frame = spark.sql("""select BIDSIZ ,SYMBOL ,date, time from TAQ where Symbol='A' and date = '20070816'""".stripMargin)



    println("=============================================")
    frame.show()
    println(frame.collect().length)
    println("=============================================")



    val endTime = System.currentTimeMillis()
    println("    =======结束时间=====   " + endTime)
    println("    =======所有时间=====   " + (endTime - beginTime ))

    spark.stop()
  }

}
