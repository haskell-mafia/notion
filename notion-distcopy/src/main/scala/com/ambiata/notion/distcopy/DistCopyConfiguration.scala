package com.ambiata.notion.distcopy

import com.ambiata.mundane.io._
import com.ambiata.saws.core._
import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import MemoryConversions._

import org.apache.hadoop.conf.Configuration

case class DistCopyConfiguration(
    hdfs: Configuration
  , client: AmazonS3Client
  , mappersNumber: Int
  , partSize: BytesQuantity
  , readLimit: BytesQuantity
  , multipartUploadThreshold: BytesQuantity
)

object DistCopyConfiguration {

  /** Defaults derived from experimentation of
      transfering files of size 15 gb.        */
  val Default = DistCopyConfiguration(
      new Configuration
    , Clients.s3
    , 1
    , 100.mb
    , 100.mb
    , 100.mb
  )
}
