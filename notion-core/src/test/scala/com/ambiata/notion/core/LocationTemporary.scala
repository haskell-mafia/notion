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

import scalaz._, Scalaz._

case class LocationTemporary(t: T, path: String, client: AmazonS3Client, conf: Configuration) {
  override def toString: String =
    s"LocationTemporary($t, $path)"

  def location: RIO[Location] = t match {
    case (T.Posix) =>
      localLocation
    case (T.S3) =>
      s3Location
    case (T.Hdfs) =>
      hdfsLocation
  }

  def io: RIO[LocationIO] =
    LocationIO(conf, client).pure[RIO]

  def hdfsLocation: RIO[Location] =
    HdfsTemporary(path).path.map(p =>
      HdfsLocation(p.toString)).run(conf).map(l => l)

  def s3Location: RIO[Location] =
    S3Temporary(path).address.map(s =>
      S3Location(s.bucket, s.key)).run(client).map(_._2)

  def localLocation: RIO[Location] =
    LocalTemporary(path).file.map(f => LocalLocation(f.path))
}

object LocationTemporary {
  implicit def LocationTemporaryArbitrary: Arbitrary[LocationTemporary] =
    Arbitrary(arbitrary[T].map(LocationTemporary(_, java.util.UUID.randomUUID().toString, Clients.s3, new Configuration)))
}
