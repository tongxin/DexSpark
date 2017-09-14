package Network

import java.nio.{ByteBuffer, ByteOrder}

import org.zeromq.ZMQ

/**
  * Created by caimy on 2017/1/2.
  */

  //封装ZeroMQ提供的数据发送和接收接口，提供DEX支持的数据类型的数据发送和接收接口
object ZmqSocket {
  var sendbuf: Array[Byte] = Array.fill(4096)(0)
  val recvbuf: Array[Byte] = Array.fill(4096)(0)
  var sbi: Int = 0
  var rbi: Int = 0

  def recv(socket: ZMQ.Socket): Unit = {
    val n = socket.recv(recvbuf, rbi, sendbuf.length - sbi, 0)


  }

  def recvByte(): Byte = {
    RecvBuf.get()
  }

  def recvInt(): Int = {
    RecvBuf.getInt
  }

  def recvString(): String = {
    val sb = new StringBuffer()
    var c = RecvBuf.get().toChar
    while (c != '\0') {
      sb.append(c)
      c = RecvBuf.get.toChar
    }
    sb.toString
  }

  def recvIntList(): Array[Int] = {
    val length = recvByte().toInt
    val intArray: Array[Int] = new Array[Int](length)
    (0 to length).foreach(
      (i: Int) =>
        intArray(i) = recvInt()
    )
    intArray
  }

  def recvStrList(): Array[String] = {
    val length = recvByte().toInt
    println("list:" + length)
    val strArray: Array[String] = new Array[String](length)
    (0 to length - 1).foreach(
      (i: Int) => {
        strArray(i) = recvString()
        println(strArray(i))
      }
    )
    println(strArray)
    strArray
  }

  def send(socket: ZMQ.Socket): Unit = {
    val bytes: Array[Byte] = new Array[Byte](1024)
    SendBuf.position(0)
    SendBuf.get(bytes)
    socket.send(bytes)
    SendBuf.clear()
  }

  def sendByte(msg: Byte): Unit = {
    SendBuf.put(msg)
  }

  def sendInt(msg: Int): Unit = {
    SendBuf.putInt(msg)
  }

  def sendString(msg: String): Unit = {
    val sb: StringBuilder = new StringBuilder(msg)
    sb.append('\0')
    println(sb.toString())
    SendBuf.put(ByteBuffer.wrap(sb.toString.getBytes()))
    println(SendBuf.position())
  }

  def sendIntList(intArray: Array[Int]): Unit = {
    sendByte(intArray.length.toByte)
    intArray.foreach(
      (i: Int) => sendInt(i)
    )
  }

  def sendStrList(strArray: Array[String]): Unit = {
    sendByte(strArray.length.toByte)
    strArray.foreach(
      (i: String) => sendString(i)
    )
  }
}
