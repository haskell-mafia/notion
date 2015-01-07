package com.ambiata.notion.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.saws.s3._
import com.ambiata.com.amazonaws.services.s3.AmazonS3Client

import java.io._

import scalaz._, Scalaz._, effect.IO

object S3OutputStream {
  def stream(address: S3Address, client: AmazonS3Client): RIO[OutputStream] = for {
    t <- LocalTemporary.random.setup.pure[RIO]
    d = t._1
    p = t._2.toFilePath
    _ <- Directories.mkdirs(p.dirname)
    o <- RIO.safe[OutputStream]({
      val f = new BufferedOutputStream(new FileOutputStream(p.path))
      new OutputStream {
        override def close(): Unit = {
          (RIO.addFinalizer(Finalizer(Directories.delete(d).void)) >>
            RIO.safe[Unit](f.close) >>
              address.putFile(p).execute(client)).unsafePerformIO
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
  } yield o

}
