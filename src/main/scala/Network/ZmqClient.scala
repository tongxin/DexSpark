package Network

import java.lang.reflect.Field

import base.DexMethod
import org.zeromq.ZMQ

/**
  * Created by caimy on 2017/1/4.
  */
 
 //基于ZeroMQ的客户端的建立
class ZmqClient extends Zmq{
  override val socket = context.socket(ZMQ.REQ)
  override def init(port:Int ): Unit ={
    val s= "tcp://*:"+ port
    socket.connect(s)
  }

  def sendMsgType(msgtype:Int): Unit ={
    ZmqSocket.sendInt(msgtype)
  }

 //处理不同消息类型的消息内容
  def handle(obj:Object): Object ={
    obj.getClass.getDeclaredFields.foreach(
      (i:Field) => {
        putMsg(DexMethod.invokeGetMethod(obj, i.getName, null))
      }
    )
    getMsg("String")
  }
}
