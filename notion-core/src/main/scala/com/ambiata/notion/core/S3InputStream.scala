package com.ambiata.notion.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.io.Temporary._
import com.ambiata.saws.s3._

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client

import java.io._

import scalaz._, Scalaz._, effect.IO

/* NB: Not production ready.
   Incomplete implementation of S3InputStream. Ideally we would like to be able
   to stream directly from an S3Address, which would allow us to disregard the
   overhead of download the file to a temporary location.

   See `263f814 # S3InputStream` for desired implementation
 */
object S3InputStream {
  def stream(address: S3Address, client: AmazonS3Client): RIO[InputStream] = {
    val tmpPath = uniqueLocalPath
    address.getFile(tmpPath).execute(client) >>
      RIO.safe[InputStream](
        new LocalInputStream(tmpPath.path.path) {
          override def close(): Unit = {
            tmpPath.delete.unsafePerformIO
            super.close()
          }
        }
      )
  }
}
