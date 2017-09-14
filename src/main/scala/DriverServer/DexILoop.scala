/**
  * DexILoop
  * 继承自SparkILoop
  * 实现循环等并执行in0输入流中的语句，并向out输出流返回结果
  */
package DriverServer

import java.io.BufferedReader

import org.apache.spark.repl.SparkILoop

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter._
import scala.tools.nsc.util._

/**
  * Created by caimy on 2017/4/11.
  */
class DexILoop (in0: Option[BufferedReader], out: JPrintWriter)
  extends SparkILoop(in0, out){
  def this(in0: BufferedReader, out: JPrintWriter) = this(Some(in0), out)
  def this() = this(None, new JPrintWriter(Console.out, true))

 //重新构造Spark的初始化函数，添加import包
  override def initializeSpark() {
    intp.beQuietDuring {
      processLine("""
        @transient val spark = if (DriverServer.DexMain.sparkSession != null) {
            DriverServer.DexMain.sparkSession
          } else {
            DriverServer.DexMain.createSparkSession
          }
        @transient val sc = {
          val _sc = spark.sparkContext
          _sc.uiWebUrl.foreach(webUrl => println(s"Spark context Web UI available at ${webUrl}"))
          println("Spark context available as 'sc' " +
            s"(master = ${_sc.master}, app id = ${_sc.applicationId}).")
          println("Spark session available as 'spark'.")
          _sc
        }
                  """)
      processLine("import org.apache.spark.SparkContext._")
      processLine("import spark.implicits._")
      processLine("import spark.sql")
      processLine("import org.apache.spark.sql.functions._")
      replayCommandStack = Nil // remove above commands from session history.
    }
  }
}

object DexILoop {

  /**
    * Creates an interpreter loop with default settings and feeds
    * the given code to it as input.
    */
  def run(code: String, sets: Settings = new Settings): String = {
    import java.io.{ BufferedReader, StringReader, OutputStreamWriter }

    stringFromStream { ostream =>
      Console.withOut(ostream) {
        val input = new BufferedReader(new StringReader(code))
        val output = new JPrintWriter(new OutputStreamWriter(ostream), true)
        val repl = new SparkILoop(input, output)

        if (sets.classpath.isDefault) {
          sets.classpath.value = sys.props("java.class.path")
        }
        repl process sets
      }
    }
  }
  def run(lines: List[String]): String = run(lines.map(_ + "\n").mkString)
}
