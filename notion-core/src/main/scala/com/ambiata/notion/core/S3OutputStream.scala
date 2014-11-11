package com.ambiata.notion.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.io.Temporary._
import com.ambiata.mundane.io.TemporaryFilePath._
import com.ambiata.saws.s3._
import com.ambiata.com.amazonaws.services.s3.AmazonS3Client

import java.io._

import scalaz._, Scalaz._, effect.IO

object S3OutputStream {
  def stream(address: S3Address, client: AmazonS3Client): ResultTIO[OutputStream] = {
    val tmpPath = uniqueFilePath
    ResultT.safe[IO, OutputStream]({
      val f = new BufferedOutputStream(new FileOutputStream(tmpPath.path))
      new OutputStream {
        override def close(): Unit = {
          runWithFilePath(tmpPath) { file =>
            ResultT.safe[IO, Unit](f.close) >>
              address.putFile(file).executeT(client)
          }.run.unsafePerformIO
          ()
        }

        override def write(i: Int): Unit =
          f.write(i)

        override def write(b: Array[Byte]): Unit =
          f.write(b)

        override def write(b: Array[Byte], off: Int, len: Int): Unit =
          f.write(b, off, len)
      }
    })
  }
}
