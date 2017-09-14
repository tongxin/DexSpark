package Network

import java.io.{BufferedReader, PrintWriter}
import java.lang.reflect.Field
import java.nio.{ByteBuffer, ByteOrder, CharBuffer}
import java.nio.charset.Charset

import Configuration.DexConfig
import base.DexMethod
import org.zeromq.ZMQ

class DexZmq(port: Int) extends Zmq {

  override val socket = context.socket(ZMQ.REP)
  socket.bind("tcp://" + DexConfig.getMaster() + ":" + port)
  socket.setIPv4Only(true)
  socket.setTCPKeepAlive(1)

  def getMsgType(): Int = getMsg[Int]

  def getRepFromStream(bufferedreader: BufferedReader): String = {
    val rep = new StringBuilder
    var tmp = bufferedreader.readLine()
    while (tmp.length < 7 || !tmp.substring(0, 7).equals("scala> ")) {
      println(tmp)
      rep.append(tmp + "\n")
      tmp = bufferedreader.readLine()
    }
    println(tmp)

    rep.toString()
  }

 //根据不同的消息类型调用各类型自带的响应操作
  def handle[T](obj: T, bufferedreader: BufferedReader, printWriter: PrintWriter): Unit = {
//    obj.getClass.getDeclaredFields.foreach(
//      (i: Field) => {
//        DexMethod.invokeSetMethod(obj, i.getName, getMsg(i.getType.toString), null)
//        println(DexMethod.invokeGetMethod(obj, i.getName, null))
//      }
//    )
    getMsg[T](obj)
    val requestHandler = obj.getClass.getMethod("handler")
    val command: String = requestHandler.invoke(obj).toString
    //    println("command: " + command)
    printWriter.println(command)
    printWriter.flush()
    //    println("printWriter end")

    val rep = getRepFromStream(bufferedreader)
    val responseHandler = obj.getClass.getMethod("respond", rep.getClass)
    val finalRep = responseHandler.invoke(obj, rep).toString
    putMsg(finalRep)
    //    println("exit handle")
  }
}

