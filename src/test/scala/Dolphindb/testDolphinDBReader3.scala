package Dolphindb

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

object testDolphinDBReader3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DolphinDBConn3")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val startTime = System.currentTimeMillis()
    val sd = spark.read.format("com.dolphindb.spark.DolphinDBProvider")
//      .option("ip", "115.239.209.224")
      .option("ip", "192.168.1.201")
//      .option("port", 16961)
      .option("port", 1337)
      .option("user", "admin")
      .option("password", "123456")
//      .option("tbPath" , "/mnt/data/dolphin_verticaa/data/db/taq")
      .option("tbPath" , "/mnt/data/spark_test_db/db3/tb")
//      .option("tbPath" , "dfs://chao/TAQ/db/taq")
//      .option("tbPath" , "dfs://chao/Spark/db/sparktb1")
      .load()
//    val ds = sd.filter("sd > 9")
//    ds.show()
//      sd.select("")


    sd.printSchema()
    sd.createTempView("sd1")

//    val frame = spark.sql("select Symbol, Date,time,  BID,OFR, BIDSIZ, OFRSIZ, MODE,EX, MMID  from sd1")
//    val frame = spark.sql("select time  from sd1 where ofr = 1 or ofr=0 order by time desc")

//    val frame = spark.sql(
//        s"""select BIDSIZ ,SYMBOL ,date, time from sd1
//              where date = to_date("2007-08-01") and symbol = 'IBM' and time > to_timestamp("09:30")
//              | """.stripMargin)

//    val frame = spark.sql("""select * from sd1 where val_string like '%1' or val_symbol like '%1' """)

//    val frame = spark.sql("""select * from sd1 where val_date in (to_date("1970-01-05") , to_date("1970-01-06")) or val_date <= to_date("1970-01-05") """)


/**    val frame = spark.sql("""select * from sd1 where val_char = '1' """) */

//    val frame = spark.sql("""select * from sd1 where val_time < to_timestamp("1970-01-01 00:05:45") """)

//    val frame = spark.sql("""select * from sd1  where isnull(val_null)""")

    val frame = spark.sql("""select * from sd1 where val_string > "\r23"  """)

//    val frame = spark.sql("""select * from sd1 where val_date in (to_date('1970-01-05'),to_date('1970-01-06'))""")


//    val frame = spark.sql(
//      """select sum(bid*bidsiz)/sum(bidsiz) as vwab , minute(time) minn from sd1
//        |where time > to_timestamp("1970-01-01 09:30:00") and
//        | time < to_timestamp("1970-01-01 10:00:00")
//        |  group by minn having sum(bidsiz) > 0 order by minn""".stripMargin)


//    frame.printSchema()

//    frame.dropDuplicates()
//    frame.distinct()
//    frame.drop()
//    frame.foreachPartition(x => println( "PARTITIONID : " + TaskContext.getPartitionId()))


//    println(frame.select("date").take(1).take(1)(0).getDate(0).getYear)
//    frame.persist()
//    val fa = frame.where($"date" >= "2007-08-02")
    println("==============frame===============")
    frame.cache()


//    frame.foreachPartition( x=>  println( "分区 ：  " + TaskContext.getPartitionId()))

//    frame.foreach(x => println(x.getAs[String]("date")))
//      frame.foreach(row => {
//          println(row)
//      })




//    println(frame.collect().length)
    frame.show()
    println("==============frame===============")
    println("ALLSIZE : " + frame.collect().length)

//    val fa = frame.filter($"date" > "2007-08-02")
//    fa.show()


    val endTime = System.currentTimeMillis()
    println("=====ALL TIME====" + (endTime - startTime))
    println("============================")
    spark.stop()

//    frame.write.format("com.dolphindb.spark.DolphinDBProvider").options()

  }

}
