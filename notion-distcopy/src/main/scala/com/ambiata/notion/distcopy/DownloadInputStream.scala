package com.ambiata.notion.distcopy

import java.io._

case class DownloadInputStream(in: InputStream, tick: Int => Unit) extends InputStream {

  def read: Int = {
    tick(1)
    in.read
  }

  override def read(bytes: Array[Byte]): Int = {
    val r = in.read(bytes)
    tick(r)
    r
  }

  override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
    val r = in.read(bytes, off, len)
    tick(r)
    r
  }

  override def close(): Unit =
    in.close()
}
