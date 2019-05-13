### 1.Spark-DolphinDB Connector 概述
Spark-DolphinDB Connector主要是使用Spark 框架来分析DolphinDB中的数据，DolphinDB 是一个分布式存储与分析的数据库。
此版本 Spark-DolphinDB Connector 主要是读取 DolphinDB中的数据，构建为Spark中的DataFrame。

Spark-DolphinDB Connector 需要运行在Java 1.8 或者以上环境， Scala 2.11或者以上环境。需要使用 
DolphinDB Java Api https://github.com/dolphindb/api-java 。
使用Spark-DolphinDB Connector 读取DolphinDB数据库中的数据，需要指定DolphinDB为data source。
 

### 2.Spark读取DolphinDB数据

创建SparkSession

    val spark = SparkSession.builder().getOrCreate()

指定DolphinDB 为Data Source,并且指定DolphinDB 数据节点的位置, ip : 数据节点的IP地址， port：数据节点的端口， 
user：登录DolphinDB数据库的用户名，password：登录DolphinDB数据库的密码， tbPath：DolphinDB数据库表的位置，
下面示例 dfs://dolphinDB/spark/db 为数据库的位置，tb1 是表名。

    val frame = spark.read.format("com.dolphindb.spark.DolphinDBProvider")
               .option("ip", "192.168.1.123")
               .option("port", 12345)
               .option("user", "admin")
               .option("password", "123456")
               .option("tbPath" , "dfs://dolphinDB/spark/db/tb1")
               .load()
               
以上得到一个Spark 中的DataFrame可以进行数据分析，也可以注册为一个临时视图使用SparkSQL来查询分析。

    val frame1 = frame.select("col1").filter("col2 > 20")
 或者
 
    frame.createOrReplaceTempView("frameTb")
    val frame2 = spark.sql("select col1 from frameTb where col2 > 20")

### 3.使用方法

可以使用 IDE 把Spark-DolphinDB Connector 的 spark-dolphindb.jar 与 DolphinDB Java Api 的 dolphindb.jar 都打包起来执行。也可以
提前把 spark-dolphindb.jar 与 dolphindb.jar发送到 $SPARK_HOME/jars 目录下，然后执行程序。
    
DolphinDB 与Spark 数据类型的不同，具体的数据类型对照参照附录。 需要注意DolphinDB中的时间类型在Spark对应为时间戳，
前面要加上 1970-01-01

查询语句示例：

 ```
    spark.sql("select col1 from frameTb where date_val > to_date('2012-12-12')")
    spark.sql("select col1 from frameTb where month_val > to_date('2012-12-01')")
    spark.sql("select col1 from frameTb where time_val > to_timestamp('1970-01-01 00:12:00')")
    spark.sql("select col1 from frameTb where minute_val > to_timestamp('1970-01-01 00:12:01')")
    spark.sql("select col1 from frameTb where second_val > to_timestamp('1970-01-01 00:12:01')")
    spark.sql("select col1 from frameTb where dataTime_val > to_timestamp('2012-12-12 00:12:01')")
    spark.sql("select col1 from frameTb where timestamp_val > to_timestamp('2012-12-12 00:12:01.123')")
    spark.sql("select col1 from frameTb where nanoTime_val > to_timestamp('1970-01-01 00:12:01.123001002')")
    spark.sql("select col1 from frameTb where nanoTimestamp_val > to_timestamp('2012-12-12 00:12:01.123001002')")
    spark.sql("select col1 from frameTb where isnull(null_val)")
    spark.sql("select col1 from frameTb where float_val = float(3.2)")
 ```
 
### 4. Spark读取DolphinDB性能提升

Spark-DolphinDB Connector 实现了分区修剪，列修剪，谓词下推，数据本地性等优化。为了实现数据本地性，需要了解DolphinDB数据库的设计。
DolphinDB 是分布式数据库，数据分布存储在不同节点上，为了使数据加载更快。建议使用内网搭建DolphinDB集群与Spark集群，
在部署 DolphinDB数据节点的机器上部署相应的Spark Slave节点。运行Spark程序可以加载本地的数据，实现数据本地性。建议使用内存大于加载的数据集。

#### 附录：DolphinDB与Spark中的数据类型

DolphinDB中具有丰富的数据类型，但是Spark中的数据类型比较有限。下表是DolphinDB对应的Spark数据类型
    
  |   DolphinDB   |            Example            |     Spark      |            Example            |
  |--------------|---------------------------|---------------|----------------------------|
  | SYMBOL        | "abc"                         | StringType    | "abc"                         |
  | STRING        | "abc"                         | StringType    | "abc"                         |
  | DATE          | 2012.12.12                    | DateType      | 2012-12-12                    |
  | MONTH         | 2012.06M                      | DateType      | 2012-06-01                    |
  | TIME          | 13:30:23.009                  | TimestampType | 1970-01-01 13:30:23.009       |
  | MINUTE        | 13:30m                        | TimestampType | 1970-01-01 13:30:01           |
  | SECOND        | 13:30:02                      | TimestampType | 1970-01-01 13:30:02           |
  | DATETIME      | 2016.06.13T13:30:29           | TimestampType | 2016-06-13 13:30:29           |
  | TIMESTAMP     | 2012.06.13T13:30:10.008       | TimestampType | 2012-06-13 13:30:10.008       |
  | NANOTIME      | 13:30:10.008007006            | TimestampType | 1970-01-01 13:30:10.008007006 |
  | NANOTIMESTAMP | 2012.06.13T13:30:10.008007006 | TimestampType | 2012-06-13 13:30:10.008007006 |
  | BOOL          | true                          | BooleanType   | true                          |
  | VOID          | NULL                          | NullType      | null                          |
  | DOUBLE        | 9.9                           | DoubleType    | 9.9                           |
  | FLOAT         | 9.9f                          | FloatType     | float(9.9)                    |
  | LONG          | 22l                           | LongType      | 22                            |
  | INT           | 22                            | IntegerType   | 22                            |
  | SHORT         | 22h                           | ShortType     | 22                            |
  | CHAR          | 'a'                           | StringType    | "a"                           |
 