package DexServer

import Configuration.DexConfig
import DexProtocol.{DexConnect, MsgType}
import Network.{ZmqBroker, ZmqBrokerServer, DexZmq}


/**
  * Created by caimy on 2017/1/11.
  */

object DexServer {
  //启动DexServer等待数据库的连接
  def main(args: Array[String]) {
    val server = new DexZmq(DexConfig.getDexServerFrontendPort())
    while (true) {
      var s: String = new String
      val msgtyp = server.recv().getMsgType()
      msgtyp match {
       //接收连接请求时，启动DriverServer进行查询服务
        case MsgType.Dex_Connect => {
          val conn = server.getMsg[DexConnect](new DexConnect(DexConfig.getMaster))
          conn.handler(server)
        }
        case _ => {
          println("error type:" + msgtyp)
          server.putMsg("Message type was not Dex_Connect. Close...").send()
        }
      }
    }
  }
}

