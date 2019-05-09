package Dolphindb

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

object testDolphinDBReader2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DolphinDBConn2")
      .master("local[2]")
      .getOrCreate()

    val startTime = System.currentTimeMillis()
    val sd = spark.read.format("com.dolphindb.spark.DolphinDBProvider")
      .option("ip", "115.239.209.224")
      .option("port", 16961)
      .option("user", "admin")
      .option("password", "123456")
//      .option("tbPath" , "dfs://chao/TAQ/db/taq")
      .option("tbPath" , "dfs://chao/Spark/db/sparktb1")
      .load()

    sd.printSchema()
    sd.createTempView("sd1")

//    val frame = spark.sql("select Symbol, Date,time,  BID,OFR, BIDSIZ, OFRSIZ, MODE,EX, MMID  from sd1")
//    val frame = spark.sql("select time  from sd1 where ofr = 1 or ofr=0 order by time desc")

    val frame = spark.sql(
        s"""select BIDSIZ ,SYMBOL ,date, time from sd1
              | where date = to_date("2007-08-01") and symbol = 'A'
              | """.stripMargin)
    frame.printSchema()

    frame.foreachPartition(x => println( "PARTITIONID : " + TaskContext.getPartitionId()))


//    println(frame.select("date").take(1).take(1)(0).getDate(0).getYear)
//    frame.persist()
//    val fa = frame.where($"date" >= "2007-08-02")
    println("==============frame===============")



    frame.show()
//    frame.foreachPartition( x=>  println( "分区 ：  " + TaskContext.getPartitionId()))

//    frame.foreach(x => println(x.getAs[String]("date")))
//      frame.foreach(row => {
//          println(row)
//      })




    println(frame.collect().length)
    println("==============frame===============")

//    val fa = frame.filter($"date" > "2007-08-02")
//    fa.show()


    val endTime = System.currentTimeMillis()
    println("=====ALL TIME====" + (endTime - startTime))
    println("============================")
    spark.stop()

//    frame.write.format("com.dolphindb.spark.DolphinDBProvider").options()

  }

}
