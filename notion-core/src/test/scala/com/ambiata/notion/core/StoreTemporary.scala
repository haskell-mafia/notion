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

import scalaz.{Store => _}

case class StoreTemporary(t: T, seed: String, client: AmazonS3Client, conf: Configuration) {
  override def toString: String =
    s"StoreTemporary($t, $seed, AmazonS3Client(...), Configuration(...))"

  def store: RIO[Store[RIO]] = t match {
    case (T.Posix) =>
      posixStore
    case (T.S3) =>
      s3Store
    case (T.Hdfs) =>
      hdfsStore
  }

  def hdfsStore: RIO[Store[RIO]] = for {
    c <- ConfigurationTemporary.random.conf
    r <- HdfsTemporary(HdfsTemporary.hdfsTemporaryPath, seed).path.map(p =>
        HdfsStore(c, p)).run(c)
  } yield r

  def s3Store: RIO[Store[RIO]] =
    S3Temporary(seed).prefix.map(p =>
      S3Store(p, client)).run(client).map(_._2)

  def posixStore: RIO[Store[RIO]] =
    LocalTemporary(Temporary.uniqueLocalPath, seed).path.map(PosixStore.apply)
}

object StoreTemporary {
  implicit def StoreTemporaryArbitrary: Arbitrary[StoreTemporary] =
    Arbitrary(arbitrary[TemporaryType].map(StoreTemporary(_, java.util.UUID.randomUUID().toString, Clients.s3, new Configuration)))
}
