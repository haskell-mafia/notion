package com.ambiata.notion.core

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.com.amazonaws.services.s3.model._

import org.apache.hadoop.fs.FSInputStream

class S3InputStream(bucket: String, key: String, contentLength: Long, client: AmazonS3Client) extends FSInputStream {
  var wrappedStream: S3ObjectInputStream = null
  var pos: Long = 0

  def openIfNeeded() =
    if (wrappedStream == null)
      reOpen(0)

  def reOpen(i: Long): Unit = {
    if (wrappedStream != null)
      wrappedStream.abort()
    val request:GetObjectRequest = new GetObjectRequest(bucket, key)
    request.setRange(pos, contentLength-1)
    wrappedStream = client.getObject(request).getObjectContent
    pos = i
  }

  override def close(): Unit =
    wrappedStream.close()

  override def read(): Int = {
    openIfNeeded()
    val rr = wrappedStream.read
    if (rr >= 0) pos = pos + 1
    rr
  }

  override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
    openIfNeeded()
    val rr = wrappedStream.read(bytes, off, len)
    if (rr > 0) pos = pos + rr
    rr
  }

  override def seek(i: Long): Unit =
    if (pos == i) ()
    else reOpen(i)

  override def getPos(): Long =
    pos

  override def seekToNewSource(i: Long) =
    false

  override def markSupported: Boolean =
    false
}
