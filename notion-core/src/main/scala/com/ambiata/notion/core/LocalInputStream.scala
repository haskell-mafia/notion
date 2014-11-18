package com.ambiata.notion.core

import org.apache.hadoop.fs.FSInputStream

import java.io._

class LocalInputStream(path: String) extends FSInputStream {
  val fis = new FileInputStream(path)
  val bis = new BufferedInputStream(fis)
  var pos: Long = 0

  override def close(): Unit =
    bis.close()

  override def read(): Int = {
    val i = bis.read
    pos = pos + 1
    i
  }

  override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
    val i = bis.read(bytes, off, len)
    pos = pos + i
    i
  }

  override def getPos(): Long =
    pos

  override def seek(i: Long): Unit = {
    fis.getChannel().position(i)
    pos = i
  }

  override def seekToNewSource(i: Long): Boolean =
    false
}
