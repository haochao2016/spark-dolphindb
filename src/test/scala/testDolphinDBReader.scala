
import java.sql.Date
import org.apache.spark.sql.SparkSession

object testDolphinDBReader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DolphinDBConn")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val sd = spark.read.format("com.dolphindb.spark.DolphinDBProvider")
      .option("ip", "115.239.209.224")
      .option("port", 16961)
      .option("user", "admin")
      .option("password", "123456")
      .option("tbPath" , "dfs://chao/Spark/db/sparktb1")
      .load()

    sd.printSchema()
    sd.createTempView("sd1")

//    val frame = spark.sql("select Symbol, Date,time,  BID,OFR, BIDSIZ, OFRSIZ, MODE,EX, MMID  from sd1")
//    val frame = spark.sql("select time  from sd1 where ofr = 1 or ofr=0 order by time desc")

    val frame = spark.sql(
//              |date="2007-08-01"
//              | date = ${date}
             /* | mode = 12
              | or bid = 15.054*/
      s"""select BIDSIZ ,SYMBOL ,date, time from sd1 where
              | Symbol='A'
              | and date > to_date('2007-08-02')
              | """.stripMargin)
    frame.printSchema()
    frame.persist()
//    val fa = frame.where($"date" >= "2007-08-02")
    println("==============frame===============")
//    println(frame.collect().length)
    frame.show()
    println("==============frame===============")

//    val fa = frame.filter($"date" > "2007-08-02")
//    fa.show()



    println("============================")
    spark.stop()


  }

}
