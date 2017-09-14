package Configuration

import java.io.File
import com.typesafe.config.ConfigFactory

/**
  * Created by caimy on 2017/1/11.
  */
object DexConfig {
 //利用ConfigFactory获取dex服务器端的配置信息
  val config = ConfigFactory.load("dex.conf")

  def getDexServerFrontendPort (): Int ={
    config.getInt("DexServerFrontendPort")
  }

  def getDexServerBackendPort(): Int ={
    config.getInt("DexServerBackendPort")
  }

  def getMaster():String = {
    config.getString("master")
  }

  def getPort():Int= {
    config.getInt("port")
  }

}
