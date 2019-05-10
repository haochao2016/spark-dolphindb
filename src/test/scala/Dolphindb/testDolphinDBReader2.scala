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
//      .option("ip", "115.239.209.224")
      .option("ip", "192.168.1.201")
      .option("port", 1337)
      .option("user", "admin")
      .option("password", "123456")
//      .option("tbPath" , "dfs://chao/TAQ/db/taq")
//      .option("tbPath" , "dfs://chao/Spark/db/sparktb1")
      .option("tbPath" , "/mnt/data/spark_test_db/db2/tb")
      .load()

//    sd.printSchema()
    sd.createTempView("sd1")

//    val frame = spark.sql("select Symbol, Date,time,  BID,OFR, BIDSIZ, OFRSIZ, MODE,EX, MMID  from sd1")
//    val frame = spark.sql("select time  from sd1 where ofr = 1 or ofr=0 order by time desc")

//    val frame = spark.sql(
//        s"""select BIDSIZ ,SYMBOL ,date, time from sd1 where
//              | date = to_date('2007-08-13') and symbol="A"
//              | """.stripMargin)


//    val frame = spark.sql("""select bidsiz, symbol, date,time from sd1 where
//                | date = to_date("2007-08-01") and symbol= "A" and ofr > 20
//                | and time > to_timestamp("1970-01-01 09:30:30") """.stripMargin)

//   val frame =  sd.select("bidsiz").filter("ofr > 20")
//    val frame = spark.sql("""select * from sd1 where  time==to_timestamp("1970-01-01 09:30:00","yyyy-MM-dd hh:mm:ss")""")

//    val frame = spark.sql("""select symbol, time, bid, ofr from sd1 where symbol in ("IBM", "MSFT", "GOOG", "YHOO") and bid > 0 and ofr > bid""")
//  val frame = spark.sql( """select time from sd1 where symbol='IBM' and time>= to_timestamp("1970-01-01 09:30:00") and time<= to_timestamp( "1970-01-01 16:00:00" )""".stripMargin)


//    val frame = spark.sql( """select symbol, minute(time) as minute, (max(ofr) - min(bid)) as gap from sd1 where symbol='IBM' and ofr > bid and bid > 0 group by symbol, minute order by minute""" )
//    val frame = spark.sql( """select count(*) as gap from sd1 where symbol='IBM' and ofr > bid and bid > 0  """ )

//    val frame = spark.sql("""select sum(bid*bidsiz)/sum(bidsiz) as vwab from sd1 group by symbol having sum(bidsiz)>0 order by symbol """)
    val frame = spark.sql("""select * from sd1""")

    frame.printSchema()


//    frame.foreachPartition(x => println( "PARTITIONID : " + TaskContext.getPartitionId()))


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




//    println(frame.collect().length)
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
