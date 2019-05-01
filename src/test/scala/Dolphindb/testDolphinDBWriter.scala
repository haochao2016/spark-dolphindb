package Dolphindb

import com.sequoiadb.hadoop.io.BSONWritable
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.SparkSession
import org.bson.BasicBSONObject

object testDolphinDBWriter {


  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()

    val spark = SparkSession.builder().appName("DolphinDBConn")
      .master("local[2]")
      .getOrCreate()

    val sd = spark.read.format("com.dolphindb.spark.DolphinDBProvider")
      .option("ip", "115.239.209.224")
      .option("port", 16961)
      .option("user", "admin")
      .option("password", "123456")
      .option("tbPath" , "dfs://chao/Spark/db/sparktb1")
      .load()

    sd.printSchema()
    sd.createTempView("sd1")

    val frame = spark.sql(
      s"""select * from sd1 where
         | Symbol='A' and
         |  date = to_date('2007-08-16')
         |  and BIDSIZ < 10
         | """.stripMargin)
    frame.printSchema()
    println("==============frame===============")



    println(frame.collect().length)

//    val sc = spark.sparkContext
//    var data = sc.parallelize(List((NullWritable.get(),new BSONWritable(new BasicBSONObject("name","gaoxing")))))


    frame.write.format("com.dolphindb.spark.DolphinDBProvider")
      .option("ip", "115.239.209.224")
      .option("port", 16961)
      .option("user", "admin")
      .option("password", "123456")
      .option("tbPath" , "dfs://chao/Spark/db/sparktb1")
      .save()


    val endTime = System.currentTimeMillis()
    println("=====ALL TIME====" + (endTime - startTime))
    spark.stop()

    //    frame.write.format("com.dolphindb.spark.DolphinDBProvider").options()


  }
}
