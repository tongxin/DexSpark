/**
  * DriverServer接收命令并利用Spark执行命令
  * parameters:
  * bufferedReader: 命令读入缓冲区
  * printWriter：结果写出
  * port: DriverServer启动端口号
  */
package DriverServer

import java.io.{BufferedReader, PrintWriter}

import DexProtocol._
import Network.DexZmq

/**
  * Created by caimy on 2017/1/4.
  */
class DriverServer(bufferedReader: BufferedReader, printWriter: PrintWriter, port: Int) {
  val server: DexZmq = new DexZmq(port)
  while (true) {
    server.recv()
    val msgType = server.getMsgType()
    val command = msgType match {
      //      case MsgType.Dex_Connect => server.handle(new DexConnect,bufferedReader, printWriter)
      case MsgType.Dex_DisConnect => server.getMsg[DexDisConnect](new DexDisConnect).handler()
      case MsgType.Dex_DataFrame => server.getMsg[DexDataFrame](new DexDataFrame).handler()
      case MsgType.Dex_Repartition => server.getMsg[DexRepartition](new DexRepartition).handler()
      case MsgType.Dex_Iterator => server.getMsg[DexDataFrameIterator](new DexDataFrameIterator).handler()
      case MsgType.Dex_Join => server.getMsg[DexJoin](new DexJoin).handler()
      case MsgType.Dex_Apply => server.getMsg[DexApply](new DexApply).handler()
    }
    printWriter.println(command)
    printWriter.flush()
    //    println("printWriter end")

    val rep = server.getRepFromStream(bufferedReader)
    val responseHandler = .getClass.getMethod("respond", rep.getClass)
    val finalRep = responseHandler.invoke(obj, rep).toString
    putMsg(finalRep)

    server.send()
  }
}
