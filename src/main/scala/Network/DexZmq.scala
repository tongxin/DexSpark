package Network

import java.io.{BufferedReader, PrintWriter}
import Configuration.DexConfig
import DexProtocol._
import org.zeromq.ZMQ

class DexZmq(port: Int) extends Zmq {

  override val socket = context.socket(ZMQ.REP)
  socket.bind("tcp://" + DexConfig.getMaster() + ":" + port)
  socket.setIPv4Only(true)
  socket.setTCPKeepAlive(1)

  def getMsgType(): Int = getMsg[Int]

  def getDexCmd(cmdType: Int): DexCommand = {
    cmdType match {
      case MsgType.Dex_Connect => getMsg[DexConnect](new DexConnect(DexConfig.getMaster(), this))
      case MsgType.Dex_DisConnect => getMsg[DexDisConnect](new DexDisConnect)
      case MsgType.Dex_DataFrame => getMsg[DexDataFrame](new DexDataFrame)
      case MsgType.Dex_Repartition => getMsg[DexRepartition](new DexRepartition)
      case MsgType.Dex_Iterator => getMsg[DexDataFrameIterator](new DexDataFrameIterator)
      case MsgType.Dex_Join => getMsg[DexJoin](new DexJoin)
      case MsgType.Dex_Apply => getMsg[DexApply](new DexApply)
    }
  }
}

