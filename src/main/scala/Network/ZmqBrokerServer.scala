package Network

import java.lang.reflect.Field

import Configuration.DexConfig
import base.DexMethod


/**
  * Created by caimy on 2017/1/11.
  */
 //基于ZeroMQ实现的中间代理，用于数据传输的服务器端
class ZmqBrokerServer(port: Int) extends DexZmq(port) {

  def handle[T](obj: T, server: ZmqBrokerServer): String = {

//    obj.getClass.getDeclaredFields.foreach(
//      (i: Field) => {
//        println(i.getType.toString)
//        DexMethod.invokeSetMethod(obj, i.getName, getMsg(), null)
//      }
//    )

    val obj = getMsg[T](obj)
    val requestHandler = obj.getClass.getMethod("handler", server.getClass)
    val url :String = requestHandler.invoke(obj, server).toString
    url
  }
}
