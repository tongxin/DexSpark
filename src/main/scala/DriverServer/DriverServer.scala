/**
  * DriverServer接收命令并利用Spark执行命令
  * parameters:
  * bufferedReader: 命令读入缓冲区
  * printWriter：结果写出
  * port: DriverServer启动端口号
  */
package DriverServer

import java.io.{BufferedReader, PrintWriter}

import Network.DexZmq

class DriverServer(bufferedReader: BufferedReader, printWriter: PrintWriter, port: Int) {
  val server: DexZmq = new DexZmq(port)
  def start(): Unit = {
    while (true) {
      server.recv()
      val msgType = server.getMsgType()
      val dexCmd = server.getDexCmd(msgType)
      val command = dexCmd.genCode()

      printWriter.println(command)
      printWriter.flush()

      val rep = server.getRepFromStream(bufferedReader)
      val reply = dexCmd.respond(rep)
      server.putMsg(reply)
      server.send()
    }
  }
}
