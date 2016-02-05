package com.ambiata.notion.core

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import org.apache.hadoop.conf.Configuration

sealed trait IOContext {
  override def toString: String =
    this match {
      case HdfsIOContext(_)      => "HdfsIOContext(Configuration(...))"
      case S3IOContext(_)        => "S3IOContext(AmazonS3Client(...))"
      case HdfsS3IOContext(_, _) => "HdfsS3IOContext(Configuration(...), AmazonS3Client(...))"
      case NoneIOContext         => "NoneIOContext"
    }
}

case class HdfsIOContext(conf: Configuration) extends IOContext
case class S3IOContext(client: AmazonS3Client) extends IOContext
case class HdfsS3IOContext(conf: Configuration, client: AmazonS3Client) extends IOContext
case object NoneIOContext extends IOContext
