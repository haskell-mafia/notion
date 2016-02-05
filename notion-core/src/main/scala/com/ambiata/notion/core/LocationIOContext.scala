package com.ambiata.notion.core

import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs._
import com.ambiata.saws.s3._

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import org.apache.hadoop.conf.Configuration

import scalaz._, Scalaz._

sealed trait LocationContext {
  def toLocation: Location = this match {
    case HdfsLocationContext(_, p) => HdfsLocation(p)
    case S3LocationContext(_, p)   => S3Location(p)
    case LocalLocationContext(p)   => LocalLocation(p)
  }

  def toContext: IOContext = this match {
    case HdfsLocationContext(c, _) => HdfsIOContext(c)
    case S3LocationContext(c, _)   => S3IOContext(c)
    case LocalLocationContext(_)   => NoneIOContext
  }
}

case class HdfsLocationContext(conf: Configuration, path: HdfsPath) extends LocationContext
case class S3LocationContext(client: AmazonS3Client, pattern: S3Pattern) extends LocationContext
case class LocalLocationContext(path: LocalPath) extends LocationContext

object LocationContext {

  def fromContextLocation(context: IOContext, location: Location): LocationIOError \/ LocationContext = (context, location) match {
    case (HdfsIOContext(conf),           HdfsLocation(path))  => HdfsLocationContext(conf, path).right
    case (HdfsS3IOContext(conf, client), HdfsLocation(path))  => HdfsLocationContext(conf, path).right
    case (S3IOContext(client),           HdfsLocation(path))  => LocationContextMissmatch(location, context).left
    case (NoneIOContext,                 HdfsLocation(path))  => LocationContextMissmatch(location, context).left
    case (HdfsIOContext(conf),           S3Location(pattern)) => LocationContextMissmatch(location, context).left
    case (HdfsS3IOContext(conf, client), S3Location(pattern)) => S3LocationContext(client, pattern).right
    case (S3IOContext(client),           S3Location(pattern)) => S3LocationContext(client, pattern).right
    case (NoneIOContext,                 S3Location(pattern)) => LocationContextMissmatch(location, context).left
    case (HdfsIOContext(conf),           LocalLocation(path)) => LocalLocationContext(path).right
    case (HdfsS3IOContext(conf, client), LocalLocation(path)) => LocalLocationContext(path).right
    case (S3IOContext(client),           LocalLocation(path)) => LocalLocationContext(path).right
    case (NoneIOContext,                 LocalLocation(path)) => LocalLocationContext(path).right
  }
}
