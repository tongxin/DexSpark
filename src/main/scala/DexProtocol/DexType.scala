package DexProtocol

import Configuration.DexConfig
import DriverServer.DexMain
import Network.{BinaryReadWrite, DexZmq, ZmqBrokerServer, ZmqSocket}
import base.{DBConnProperties, DexMethod}

import scala.beans.BeanProperty

trait DexCommand {
  def cmdType: Int
  def genCode(): String
  def respond(input: String): String
}

class DexConnect(@BeanProperty var master: String, server: DexZmq) extends DexCommand {
  def this() = this("")

  override def cmdType: Int = MsgType.Dex_Connect

  override def genCode(): String = {
    return ""
  }

  override def respond(input: String): String = {
    return "Connected."
  }

  def handle(): Unit = {
    val port = DexConfig.getPort()
    val msg = master + ":" + port.toString
    server.putMsg(msg)
    server.send()
    DexMain.start(DexConfig.getPort())
  }
}

class DexDisConnect(@BeanProperty var master: String) extends DexCommand {

  def this() = this("")

  override def cmdType: Int = MsgType.Dex_DisConnect

  override def genCode(): String = {
    ":q"
  }

  override def respond(input: String): String = {
    val dexDisConnected = "1"
    dexDisConnected
  }
}

class DexDataFrame(@BeanProperty var dataframe: String,
                   @BeanProperty var tablename: String,
                   @BeanProperty var numPartitions: Integer,
                   @BeanProperty var db: Array[String]) extends DexCommand {

  def this() = this("", "", 0, null)

  override def cmdType: Int = MsgType.Dex_DataFrame

  override def genCode(): String = {

    val dbconn = new DBConnProperties(db(0), db(1), db(2), db(3))
    val url = "jdbc:postgresql://" + dbconn.getUrl
    val user = dbconn.getUser
    val passwd = dbconn.getPasswd
    val driver = dbconn.getDriver
    println(url)
    //    val command:String = s"""val $dataframe = spark.read""" +
    //      s""".format("jdbc")""" +
    //      s""".option("url","jdbc:postgresql://$url")""" +
    //      s""".option("dbtable","$tablename") """+
    //      s""".option("user","$user")""" +
    //      s""".option("password","$passwd")""" +
    //      s""".option("numPartitions",$numPartitions)""" +
    //      s""".option("driver", "$driver").load()\n""" +
    //      s"""$dataframe.schema""
    val command: String =
      s"""import base.dexDataFrame
          |val $dataframe = new dexDataFrame(spark, "$tablename", $numPartitions, "$url", "$user", "$passwd", "$driver")
       """.stripMargin
    command
  }

  override def respond(input: String): String = {
    println("respond: " + input)
    val dexDataFrameCreated = "1"
    dexDataFrameCreated
  }
}

class DexRepartition(@BeanProperty var newDataframe: String,
                     @BeanProperty var oldDataframe: String,
                     @BeanProperty var numsPartition: Int,
                     @BeanProperty var partitionCols: Array[String]) extends DexCommand {
  def this() = this("", "", 0, null)

  override def cmdType: Int = MsgType.Dex_Repartition

  override def genCode(): String = {
    val command = s"val $newDataframe = $oldDataframe.repartition($numsPartition, $partitionCols)"
    command
  }

  override def respond(input: String): String = {
    return "1"
  }
}

class DexDataFrameIterator(@BeanProperty var iterator: String,
                           @BeanProperty var dataframe: String,
                           @BeanProperty var partitionId: Int) extends DexCommand {
  def this() = this("", "", 0)

  override def cmdType: Int = MsgType.Dex_Iterator

  override def genCode(): String = {
    val command = s"val $iterator = $dataframe.toLocalIterator"
    command
  }

  override def respond(input: String): String = {

  }
}

class DexJoin(@BeanProperty var newDataframe: String,
              @BeanProperty var dataframe1: String,
              @BeanProperty var dataframe2: String,
              @BeanProperty var cols: Array[String]) extends DexCommand {

  def this() = this("", "", "", null)

  override def cmdType: Int = MsgType.Dex_Join

  override def genCode(): String = {
    val seqCol = cols.toSeq
    val command = new StringBuilder
    command.append(s"val $newDataframe = $dataframe1.join($dataframe2,")
    if (seqCol.length > 1) {
      command.append("Seq(")
      var tmp = new String
      for (i <- 0 to seqCol.length - 2) {
        tmp = seqCol(i)
        command.append(s""""$tmp",""")
      }
      tmp = seqCol(seqCol.length - 1)
      command.append(s""""$tmp"))\n""")
    } else {
      val tmp = seqCol(0)
      command.append(s""""$tmp")\n""")
    }
    command.append(s"""$newDataframe.schema""")
    command.toString()
  }

  override def respond(input: String): String = {
    println("input:" + input)
    val rep = new StringBuilder
    val p = """StructField\((.*,.*,.*)\)""".r
    val structList = (p findFirstIn input toString).split("StructField\\(")
    rep.append(structList.length - 1)
    rep.append("\0")
    structList.foreach(
      s => {
        val arr = s.split(",")
        if (arr.length >= 3) {
          rep.append(arr(0) + "\0")
          arr(1) match {
            case "StringType" => rep.append(DataType.StringType + "\0")
            case "IntegerType" => rep.append(DataType.IntegerType + "\0")
            case "DoubleType" => rep.append(DataType.DoubleType + "\0")
          }
        }
      }
    )
    println(rep.toString())
    rep.toString()
  }
}

class DexUnion(@BeanProperty var newDataframe: String,
               @BeanProperty var dataframe1: String,
               @BeanProperty var dataframe2: String) extends DexCommand {

  def this() = this("", "", "")

  override def cmdType: Int = MsgType.Dex_Union

  override def genCode(): String = {
    val command = s"val $newDataframe = $dataframe1.union($dataframe2)"
    command
  }

  override def respond(input: String): String = {
    val dexDataFrameUnioned = "1"
    dexDataFrameUnioned
  }
}

class DexApply(@BeanProperty var dataframe: String,
               @BeanProperty var function: String,
               @BeanProperty var params: Array[String]) extends DexCommand {
  def this() = this("", "", null)

  override def cmdType: Int = MsgType.Dex_Apply

  override def genCode(): String = {
    println("DexApply")
    var command = new StringBuilder
    function match {
      case "kmeans" => {
        command.append(s"""$dataframe.kmeans("${params(0)}",${params(1)}, ${params(2)})""")
      }
      case "WifiUserAnalysis" => {
        command.append(s"""$dataframe.WifiUserAnalysis("${params(0)}",${params(1)}, ${params(2)})""")
      }
    }
    println(command.toString)
    command.toString()
  }

  override def respond(input: String): String = {
    val dexDataFrameUnioned = "1"
    dexDataFrameUnioned
  }
}



