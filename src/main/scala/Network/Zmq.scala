package Network

import java.nio.ByteBuffer

import org.zeromq.ZMQ

abstract class Zmq {
  val context = ZMQ.context(1)
  val socket = context.socket(0)

  //  socket state
  var sendbuf: ByteBuffer = ByteBuffer.allocateDirect(4096)
  val recvbuf: ByteBuffer = ByteBuffer.allocateDirect(4096)

  def putMsg[T](m: T): this.type = {
    DexObject.write(sendbuf, m)
    this
  }

  def send(): Unit = {
    socket.sendZeroCopy(sendbuf, sendbuf.position(), 0)
    sendbuf.position(0)
  }

  def getMsg[T](): T = {
    DexObject.read(recvbuf)
  }

  def getMsg[T](obj: T): T = {
    DexObject.read(recvbuf, obj)
  }

  def recv(): this.type = {
    recvbuf.position(0)
    val nb = socket.recvZeroCopy(recvbuf, recvbuf.capacity(), 0)
    recvbuf.flip()
    this
  }

  def close(): Unit ={
    socket.close()
    context.close()
  }
}
