package base

import Applications.{Clustering, WifiUserAnalysis, WordCount}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by caimy on 2017/3/22.
  * 适应spark的dexDataFrame,进行各种数据集操作的转化
  * SparkSession: spark运行会话
  * table: 待分析数据表
  * numPartition: 分区数
  * url :　数据库连接
  * user: 数据库用户
  * passwd: 用户登陆密码
  * driver: 数据库连接驱动
  * df: dexDataFrame的主体
  */
class DexDataframe(val spark: SparkSession,
                   val table: String,
                   val numPartitions: Int,
                   val url: String,
                   val user: String,
                   val passwd: String,
                   val driver: String,
                   val df: DataFrame) {

  def this(spark: SparkSession, table: String, numPartitions: Int, url: String, user: String, passwd: String, driver: String) {
    this(spark, table, numPartitions, url, user, passwd, driver, null)
  }

  def this(url: String, user: String, passwd: String, driver: String, df: DataFrame) {
    this(null, null, 0, url, user, passwd, driver, df)
  }

  val dbconn: DBConnProperties = new DBConnProperties(url, user, passwd, driver)
  val dataframe: DataFrame = create()

  def create(): DataFrame = {
    if (df == null) {
      val data = spark.read.format("jdbc")
        .option("url", dbconn.url)
        .option("dbtable", table)
        .option("user", dbconn.user)
        .option("password", dbconn.passwd)
        .option("numPartitions", numPartitions)
        .option("driver", dbconn.driver).load()
      data.count()
      data
    }
    else
      df
  }

  def join(df: DexDataframe, col: String): DexDataframe = {
    new DexDataframe(df.url, df.user, df.passwd, df.driver, this.dataframe.join(df.dataframe, col))
  }

  def join(df: DexDataframe, cols: Seq[String]): DexDataframe = {
    new DexDataframe(df.url, df.user, df.passwd, df.driver, this.dataframe.join(df.dataframe, cols))
  }

  def union(df: DexDataframe): DexDataframe = {
    new DexDataframe(df.url, df.user, df.passwd, df.driver, this.dataframe.union(df.dataframe))
  }

  def repartition(): DexDataframe = {
    new DexDataframe(url, user, passwd, driver, this.dataframe.repartition())
  }

  def kmeans(tablename: String, numClusters: Int, numIterations: Int): Unit = {
    new Clustering(tablename, numClusters, numIterations).compute(spark, dataframe, dbconn)
  }

  def WifiUserAnalysis(tablename: String, numClusters: Int, numIterations: Int): Unit = {
    new WifiUserAnalysis(tablename, numClusters, numIterations).compute(spark, dataframe, dbconn)
  }

  def wordcount(): Unit = {
    WordCount.compute(dataframe)
  }

}

object DexDataframe {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Hive Example")
      .getOrCreate()
    val df = new DexDataframe(spark, "lenses", 2, "jdbc:postgresql://172.20.110.61:5432/exampledb", "postgres", "123456", "org.postgresql.Driver")
  }
}
