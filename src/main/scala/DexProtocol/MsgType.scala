package DexProtocol

/**
  * Created by caimy on 2017/1/5.
  */
object MsgType extends Enumeration {
  type MsgType = Int
  val Dex_Connect = 1
  val Dex_DisConnect = 2
  val Dex_Session = 3
  val Dex_DataFrame = 4
  val Dex_Repartition = 5
  val Dex_Iterator = 6
  val Dex_Join = 7
  val Dex_Union = 8
  val Dex_Apply = 9
}

object DataType extends Enumeration {
  type DataType = Int
  val StringType = 1
  val IntegerType = 2
  val DoubleType = 3
}
