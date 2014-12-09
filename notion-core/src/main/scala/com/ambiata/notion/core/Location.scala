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

  def render: String = path

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
  def render: String = bucket+"/"+key

  def map(f: DirPath => DirPath): Location =
    copy(key = f(DirPath.unsafe(key)).path)
}

case class LocalLocation(path: String) extends Location {

  def dirPath  = DirPath.unsafe(path)
  def filePath = FilePath.unsafe(path)

  def render: String = path

  def map(f: DirPath => DirPath): Location =
    copy(path = f(dirPath).path)

}

object LocalLocation {
  def create(dir: DirPath): LocalLocation =
    LocalLocation(dir.path)
}

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
}
