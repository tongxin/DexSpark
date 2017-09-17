/**
  * 重新构建org.apache.spark.repl.IMain函数
  * 成功建立连接启动该对象
  * 将spark-shell从终端获取命令语句修改为从pipe管道流中启动
  * 启动DriverServer接收命令
  */
package DriverServer

import java.io._

import Configuration.DexConfig
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.repl.SparkILoop
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.Future
import scala.tools.nsc.GenericRunnerSettings
import scala.concurrent.ExecutionContext.Implicits.global


/**
  * Created by caimy on 2017/1/4.
  */
object DexMain{
  val conf = new SparkConf()

  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _
  private var hasErrors = false

  var interp: SparkILoop = _

  //管道流通信，通过两组管道实现Spark从网络流获取命令并将结果返回给网络流
  val posDriver: PipedOutputStream = new PipedOutputStream()
  val pisSpark: PipedInputStream = new PipedInputStream()
  val posSpark: PipedOutputStream = new PipedOutputStream()
  val pisDriver: PipedInputStream = new PipedInputStream()

  private def scalaOptionError(msg: String): Unit = {
    hasErrors = true
    val os = new PrintWriter(posSpark)
    os.write(msg)
    os.flush()
    os.close()
  }

  def main(args: Array[String]) {
    val port = 9999
    start(port)
  }

  def start(port: Int) {
    try {
      pisSpark.connect(posDriver)
      pisDriver.connect(posSpark)
    } catch {
      case _ => print("error")
    }
    println("new SparkILoop")
    Future {
      val br = new BufferedReader(new InputStreamReader(pisDriver))
      println(br.readLine())
      for (i <- 1 to 10) {
        println(br.readLine())
      }
      println("start: new DriverServer")
      new DriverServer(
        new BufferedReader(new InputStreamReader(pisDriver)),
        new PrintWriter(new OutputStreamWriter(posDriver)), port)
        .start()
    }

    interp = new DexILoop(
      new BufferedReader(new InputStreamReader(pisSpark)),
      new PrintWriter(new OutputStreamWriter(posSpark)))
    val settings = new GenericRunnerSettings(scalaOptionError)
    settings.usejavacp.value = true
    settings.classpath.append(DexConfig.getDexJarPath())
    interp.process(settings)
    Option(sparkContext).map(_.stop())
  }

  def createSparkSession(): SparkSession = {
    val execUri = System.getenv("SPARK_EXECUTOR_URI")
    conf.setIfMissing("spark.add.name", "Spark shell")

    if (execUri != null) {
      conf.setIfMissing("spark.executor.uri", execUri)
    }
    if (System.getenv("SPARK_HOME") != null) {
      conf.setSparkHome(System.getenv("SPARK_HOME"))
    }

    val builder = SparkSession.builder().config(conf)
    sparkSession = builder.getOrCreate()
    sparkContext = sparkSession.sparkContext
    sparkSession
  }
}
