package Applications

import java.util.Properties

import base.DBConnProperties
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types._

import scala.beans.BeanProperty


/**
  * Created by caimy on 2017/2/21.
  * 实现kmeans算法
  * parameters:
  * tablename: 用于数据分析的数据表名
  * numClusers: kmeans算法中生成簇个数
  * numIterations: kmeans算法的最高迭代次数
  */
class Cluster(@BeanProperty var tablename: String,
              @BeanProperty var numClusters: Int,
              @BeanProperty var numIterations: Int) {

  def compute(spark: SparkSession, data: DataFrame, dbconn: DBConnProperties): Unit = {
    val parsedData = data.drop(data.columns(0)).rdd.map(
      row => {
        Vectors.dense(row.toSeq.toArray.map(_.toString.toDouble))
      }
    ).cache()

    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    val predictData = data.rdd.map(
      row => {
        Row(row.getAs[String](0),
          clusters.predict(Vectors.dense(row.toSeq.toArray.drop(1).map(_.toString.toDouble))))
      }
    )

    val schema = StructType(
      Seq(
        StructField("pid", StringType, true),
        StructField("clusterid", IntegerType, true)
      )
    )

    val prop = new Properties()
    prop.put("user", dbconn.user)
    prop.put("password", dbconn.passwd)
    prop.put("driver", dbconn.driver)
    spark.createDataFrame(predictData, schema).toDF().write.mode("overwrite").jdbc(dbconn.url, tablename, prop)
  }

 //测试能否正常执行kmeans算法
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Hive Example")
      .getOrCreate()
    val data = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://172.20.110.61:5432/exampledb")
      .option("dbtable", "km_sample2_spark")
      .option("user", "postgres")
      .option("password", "123456")
      .load()
//        compute(spark,data, "predict_sample2_spark",
//          new DBConnProperties("jdbc:postgresql://172.20.110.61:5432/exampledb",
//                                  "postgres",
//                                  "123456",
//                                  "Driver.org.postgresql" ))
  }
}
