package com.ambiata.notion.core

import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core.{TemporaryType => T}
import com.ambiata.notion.core.Arbitraries._
import com.ambiata.poacher.hdfs._
import com.ambiata.saws.core._
import com.ambiata.saws.s3._

import org.apache.hadoop.conf.Configuration
import org.scalacheck._, Arbitrary._

case class LocationTemporary(t: T, seed: String, client: AmazonS3Client, conf: Configuration) {
  override def toString: String =
    s"LocationTemporary($t, $seed, AmazonS3Client(...), Configuration(...))"

  def context: IOContext = t match {
    case (T.Posix) => NoneIOContext
    case (T.S3)    => S3IOContext(client)
    case (T.Hdfs)  => HdfsIOContext(conf)
  }

  def contextAll: IOContext =
    HdfsS3IOContext(conf, client)

  def location: RIO[Location] = t match {
    case (T.Posix) =>
      localLocation
    case (T.S3) =>
      s3Location
    case (T.Hdfs) =>
      hdfsLocation
  }

  def hdfsLocation: RIO[Location] =
    HdfsTemporary(HdfsTemporary.hdfsTemporaryPath, seed).path.run(conf).map(HdfsLocation.apply)

  def s3Location: RIO[Location] =
    S3Temporary(seed).pattern.map(S3Location.apply).run(client).map(_._2)

  def localLocation: RIO[Location] =
    LocalTemporary(Temporary.uniqueLocalPath, seed).path.map(LocalLocation.apply)
}

object LocationTemporary {
  implicit def LocationTemporaryArbitrary: Arbitrary[LocationTemporary] =
    Arbitrary(arbitrary[T].map(LocationTemporary(_, java.util.UUID.randomUUID().toString, Clients.s3, new Configuration)))
}
