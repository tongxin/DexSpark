package Network

import org.zeromq.ZMQ


/**
  * Created by caimy on 2017/1/11.
  */
 //基于ZeroMQ实现的中间代理
class ZmqBroker extends Zmq{
  val frontend = context.socket(ZMQ.ROUTER)
  val backend = context.socket(ZMQ.DEALER)

  //初始化socket
  def init(frontendPort:Int, backendPort:Int): Unit ={
    frontend.bind("tcp://*:" + frontendPort)
    print(frontendPort)
    backend.bind("tcp://*:" + backendPort)
  }

  def start(frontendPort:Int, backendPort:Int): Unit = {
    init(frontendPort, backendPort)

    //初始化 server 工作池
    val items = context.poller(2)
    items.register(frontend, 1)
    items.register(backend, 1)

    var more = false
    //在dealer 和 router之间交换信息
    while (!Thread.currentThread().isInterrupted) {
      // poll and memorize multipart detection
      items.poll()

      if (items.pollin(0)) {
        do {
          //接收 requester 请求信息
          val message = frontend.recv(0)
          more = frontend.hasReceiveMore

          // 传递给 backend
          if(more)
            backend.send(message, ZMQ.SNDMORE)
          else
            backend.send(message, 0)
        } while (more)
      }
      if (items.pollin(1)) {
        do{
          //接收 worker 响应信息
          val message = backend.recv(0)
          more = backend.hasReceiveMore
          // 传递给 frontend
          if (more)
            frontend.send(message,ZMQ.SNDMORE)
          else
            frontend.send(message, 0)
        } while (more)
      }
    }
    //释放socket及zmq上下文空间
    close()
  }

  override def close(): Unit ={
    frontend.close()
    backend.close()
    context.term()
  }
}
