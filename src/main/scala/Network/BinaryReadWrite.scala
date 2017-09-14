package Network

import java.nio.ByteBuffer

trait BinaryReadWrite[T] {
  def read(bb: ByteBuffer): T
  def read(bb: ByteBuffer, obj: T): T
  def write(bb: ByteBuffer, x: T): Unit
}

object DexObject {
  object RWException extends Exception

  def read[T](bb: ByteBuffer)(implicit rw: BinaryReadWrite[T]): T = {
    rw.read(bb)
  }

  def read[T](bb: ByteBuffer, obj: T)(implicit rw: BinaryReadWrite[T]): T = {
    return rw.read(bb, obj)
  }

  def write[T](bb: ByteBuffer, x: T)(implicit rw: BinaryReadWrite[T]): Unit = {
    rw.write(bb, x)
  }

  implicit object DexInt extends BinaryReadWrite[Int] {
    override def read(bb: ByteBuffer): Int = {
      bb.getInt()
    }

    override def read(bb: ByteBuffer, obj: Int): Int = {
      throw RWException
    }

    override def write(bb: ByteBuffer, x: Int): Unit = {
      bb.putInt(x)
    }
  }

  implicit object DexIntArray extends BinaryReadWrite[Array[Int]] {
    override def read(bb: ByteBuffer): Array[Int] = {
      val len = DexInt.read(bb)
      val buf = new Array[Int](len)
      if (len > 0) {
        (0 to (len - 1)).foreach(i => buf(i) = DexInt.read(bb))
      }
      buf
    }

    override def read(bb: ByteBuffer, obj: Array[Int]): Array[Int] = {

      return obj
    }

    override def write(bb: ByteBuffer, xs: Array[Int]): Unit = {
      val len = xs.length
      bb.putInt(len)
      xs.foreach(x => bb.putInt(x))
    }
  }

  implicit object DexString extends BinaryReadWrite[String] {
    val sb = StringBuilder.newBuilder

    override def read(bb: ByteBuffer): String = {
      val len = DexInt.read(bb)
      sb.setLength(0)
      if (len > 0) {
        (0 to (len - 1)).foreach(sb.append(bb.getChar()))
      }
      sb.toString()
    }

    override def read(bb: ByteBuffer, obj: String): String = {

      return obj
    }

    override def write(bb: ByteBuffer, xs: String): Unit = {
      bb.putInt(xs.length)
      xs.foreach(x => bb.putChar(x))
    }
  }

  implicit object DexStrArray extends BinaryReadWrite[Array[String]] {
    override def read(bb: ByteBuffer): Array[String] = {
      val len = DexInt.read(bb)
      val ss = new Array[String](len)
      if (len > 0) {
        (0 to (len - 1)).foreach(i => ss(i) = DexString.read(bb))
      }
      return ss
    }

    override def read(bb: ByteBuffer, obj: Array[String]): Array[String] = {

      return obj
    }

    override def write(bb: ByteBuffer, xs: Array[String]): Unit = {
      val len = xs.length
      bb.putInt(len)
      xs.foreach(x => DexString.write(bb, x))
    }
  }

}