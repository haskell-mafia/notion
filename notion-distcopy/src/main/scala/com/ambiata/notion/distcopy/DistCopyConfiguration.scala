package com.ambiata.notion.distcopy

import com.ambiata.mundane.io._
import com.ambiata.saws.core._
import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import MemoryConversions._

import org.apache.hadoop.conf.Configuration

/**
 * Configuration for running DistCopy jobs
 */
case class DistCopyConfiguration(
    hdfs: Configuration
  , client: AmazonS3Client
  , parameters: DistCopyParameters
) {
  def mappersNumber: Int = parameters.mappersNumber
  def retryCount: Int = parameters.retryCount
  def partSize: BytesQuantity = parameters.partSize
  def readLimit: BytesQuantity = parameters.readLimit
  def multipartUploadThreshold: BytesQuantity = parameters.multipartUploadThreshold
}

object DistCopyConfiguration {
  /** create a dist copy configuration with default mapper parameters, given the number of required mappers */
  def create(mappersNumber: Int) = DistCopyConfiguration(
      new Configuration
    , Clients.s3
    , DistCopyParameters.createDefault(mappersNumber)
  )
}

/**
 * The DistCopyParameters are
 *
 *  - a number of mappers: this depends on the number and size of files being transferred
 *  - mappers parameters governing the transfer of each file between S3 and Hdfs
 */
case class DistCopyParameters(mappersNumber: Int, mapperParameters: DistCopyMapperParameters) {
  def retryCount: Int = mapperParameters.retryCount
  def partSize: BytesQuantity = mapperParameters.partSize
  def readLimit: BytesQuantity = mapperParameters.readLimit
  def multipartUploadThreshold: BytesQuantity = mapperParameters.multipartUploadThreshold
}

object DistCopyParameters {
  def createDefault(mappersNumber: Int) = DistCopyParameters(
      mappersNumber
    , DistCopyMapperParameters.Default
  )
}

/**
 * These parameters are used by each mapper to run dist-copy tasks
 *
 * For transferring files of various sizes
 *  experimentation suggests values of
 *
 *    retryCount = 3
 *    partSize = 100.mb
 *    readLimit = 100.mb
 *    multipartUploadThreshold = 100.mb
 */
case class DistCopyMapperParameters(
    retryCount: Int
  , partSize: BytesQuantity
  , readLimit: BytesQuantity
  , multipartUploadThreshold: BytesQuantity
)

object DistCopyMapperParameters {
  val Default = DistCopyMapperParameters(
      3
    , 100.mb
    , 100.mb
    , 100.mb
  )
}

