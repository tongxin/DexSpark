package Applications


import base.DBConnProperties
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.beans.BeanProperty


/**
  * Created by caimy on 2017/3/29.
  * 公交用户聚类
  * tablename: 进行数据分析的数据表名
  * numClusters: kmeans算法簇节点个数
  * numIterators: kmeans算法最高迭代次数
  */
class WifiUserAnalysis(@BeanProperty var tablename: String,
                       @BeanProperty var numClusters: Int,
                       @BeanProperty var numIterations: Int)  {
  def compute(spark: SparkSession, data: DataFrame, dbconn: DBConnProperties): Unit = {
  //将用户操作评分化
    val newdf = data.rdd.map(
      (i: Row) => {
        i.get(i.fieldIndex("actid")).toString().substring(1) match {
          case "00" => Row(i.getString(i.fieldIndex("userid")), i.getString(i.fieldIndex("moduleid")), 1.toDouble)
          case "01" => Row(i.getString(i.fieldIndex("userid")), i.getString(i.fieldIndex("moduleid")), 2.toDouble)
          case "02" => Row(i.getString(i.fieldIndex("userid")), i.getString(i.fieldIndex("moduleid")), 1.toDouble)
          case "03" => Row(i.getString(i.fieldIndex("userid")), i.getString(i.fieldIndex("moduleid")), 4.toDouble)
          case "04" => Row(i.getString(i.fieldIndex("userid")), i.getString(i.fieldIndex("moduleid")), 3.toDouble)
          case "05" => Row(i.getString(i.fieldIndex("userid")), i.getString(i.fieldIndex("moduleid")), 3.toDouble)
          case "06" => Row(i.getString(i.fieldIndex("userid")), i.getString(i.fieldIndex("moduleid")), 3.toDouble)
          case _ => Row(i.getString(i.fieldIndex("userid")), i.getString(i.fieldIndex("moduleid")), 0.toDouble)
        }
      }
    ).cache()

    //评分矩阵转置
    val schema = StructType(
      Seq(
        StructField("userid", StringType, true),
        StructField("moduleid", StringType, true),
        StructField("score", DoubleType, true)
      )
    )
    import spark.implicits._
    val scoreDF = spark.createDataFrame(newdf, schema).toDF()

    val module = scoreDF.select("moduleid").map(row => row.getAs[String]("moduleid")).distinct().collect().toList
    val matrix = scoreDF.groupBy("userid").pivot("moduleid", module).sum("score").na.fill(0)

    //      val tablename = "predict_wifidata"
    //      val numClusters = 20
    //      val numIterations = 50

    //Kmeans算法聚类
    new Cluster(tablename, numClusters, numIterations).compute(spark, matrix, dbconn)
  }

  //测试
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Hive Example")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()
    val data = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://192.168.1.111:5432/exampledb")
      .option("dbtable", "(select userid, moduleid, actid from wifidata0829 where userid <> ' ') as tmp")
      .option("user", "cai")
      .option("password", "123456")
      .option("driver", "org.postgresql.Driver")
      .load()
    compute(spark, data,
      new DBConnProperties("jdbc:postgresql://192.168.1.111:5432/exampledb",
        "postgres",
        "123456",
        "org.postgresql.Driver"))
  }
}
