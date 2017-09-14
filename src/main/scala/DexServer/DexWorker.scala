package DexServer

import java.util.concurrent.{ExecutorService, Executors}

import Configuration.DexConfig
import DexProtocol.MsgType.MsgType
import DexProtocol.{DexConnect, MsgType}
import Network.ZmqBrokerServer

/**
  * Created by caimy on 2017/1/12.
  */
class DexWorkerPool(poolSize:Int){
  val threadPool:ExecutorService = Executors.newFixedThreadPool(poolSize)
  def start(): Unit ={
    try{
      //提交线程
      for(i <- 1 to poolSize){
        threadPool.execute(new DexWorker)
      }
    }finally {
      threadPool.shutdown()
    }
  }
}

class DexWorker extends Runnable{
  def run(): Unit ={
    val zbServer = new ZmqBrokerServer
    zbServer.init(DexConfig.getDexServerBackendPort())
    zbServer.recvMsgType() match {
      case MsgType.Dex_Connect => zbServer.handle(new DexConnect,zbServer)
      case _ => println("error message type")
    }
  }
}
