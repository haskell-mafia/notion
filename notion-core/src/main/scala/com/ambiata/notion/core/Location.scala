package com.ambiata.notion.core

import scalaz._, Scalaz._
import com.ambiata.mundane.io._

/**
 * A location represents a "path" on a file system
 * on either HDFS, S3 or locally
 */
sealed trait Location {
  /** show a string representing the location path */
  def render: String

  /** modify this  */
  def map(f: DirPath => DirPath): Location

  /** extend this location with a file path */
  def </>(other: FilePath): Location = map(_ </> other.toDirPath)

  /** extend this location with a dir path */
  def </>(other: DirPath):  Location = map(_ </> other)

  /** extend this location with a file name */
  def </>(name: FileName):  Location = map(_ </> name)

}

case class HdfsLocation(path: String) extends Location {

  def render: String =
    "hdfs://"+path

  def dirPath  = DirPath.unsafe(path)
  def filePath = FilePath.unsafe(path)

  def map(f: DirPath => DirPath): Location =
    copy(path = f(dirPath).path)
}

object HdfsLocation {
  def create(dir: DirPath): HdfsLocation =
    HdfsLocation(dir.path)
}

case class S3Location(bucket: String, key: String) extends Location {
  def render: String =
    "s3://"+bucket+"/"+key

  def map(f: DirPath => DirPath): Location =
    copy(key = f(DirPath.unsafe(key)).path)
}

case class LocalLocation(path: String) extends Location {

  def dirPath  = DirPath.unsafe(path)
  def filePath = FilePath.unsafe(path)

  def render: String =
    if (path.startsWith("/")) "file://"+path
    else                      path

  def map(f: DirPath => DirPath): Location =
    copy(path = f(dirPath).path)

}

object LocalLocation {
  def create(dir: DirPath): LocalLocation =
    LocalLocation(dir.path)
}

import argonaut._, Argonaut._

object Location {
  def fromUri(s: String): String \/ Location = try {
    val uri = new java.net.URI(s)
    uri.getScheme match {
      case "hdfs" =>
        HdfsLocation(uri.getPath).right
      case "s3" =>
        S3Location(uri.getHost, uri.getPath.drop(1)).right
      case "file" =>
        LocalLocation(uri.toURL.getFile).right
      case null =>
        LocalLocation(uri.getPath).right
      case _ =>
        s"Unknown or invalid repository scheme [${uri.getScheme}]".left
    }
  } catch {
    case e: java.net.URISyntaxException =>
      e.getMessage.left
  }

  def localLocationFromUri(s: String): String \/ Location =
    try {
      val uri = new java.net.URI(s)
      uri.getScheme match {
        case "file" =>
          LocalLocation(uri.toURL.getFile).right
        case null =>
          LocalLocation(uri.getPath).right
        case _ =>
          s"Not a local location [${uri.getScheme}]".left
      }
    } catch { case e: java.net.URISyntaxException => e.getMessage.left }

  def s3LocationFromUri(s: String): String \/ Location =
    try {
      val uri = new java.net.URI(s)
      uri.getScheme match {
        case "s3" =>
          S3Location(uri.getHost, uri.getPath.drop(1)).right
        case _ =>
          s"Not a S3 location [${uri.getScheme}]".left
      }
    } catch { case e: java.net.URISyntaxException => e.getMessage.left }

  def hdfsLocationFromUri(s: String): String \/ Location =
    try {
      val uri = new java.net.URI(s)
      uri.getScheme match {
        case "hdfs" =>
          HdfsLocation(uri.getPath).right
        case _ =>
          s"Not a HDFS location [${uri.getScheme}]".left
      }
    } catch { case e: java.net.URISyntaxException => e.getMessage.left }

  implicit def LocationEncodeJson: EncodeJson[Location] =
    EncodeJson({
      case S3Location(b, k) => Json("s3"   := Json("bucket" := b, "key" := k))
      case HdfsLocation(p)  => Json("hdfs" := Json("path" := p))
      case LocalLocation(p) => Json("local":= Json("path" := p))
    })

  implicit def LocationDecodeJson: DecodeJson[Location] =
    DecodeJson(c =>
      tagged("s3",    c, jdecode2L(S3Location.apply)("bucket", "key")).map(l => l:Location) |||
      tagged("hdfs",  c, jdecode1L(HdfsLocation.apply)("path")).map(l => l:Location) |||
      tagged("local", c, jdecode1L(LocalLocation.apply)("path")).map(l => l:Location))

  def tagged[A](tag: String, c: HCursor, decoder: DecodeJson[A]): DecodeResult[A] =
    (c --\ tag).hcursor.fold(DecodeResult.fail[A]("Invalid tagged type", c.history))(decoder.decode)

  implicit def LocationEqual: Equal[Location] =
    Equal.equalA
}
