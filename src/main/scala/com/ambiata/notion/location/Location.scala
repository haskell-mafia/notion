package com.ambiata.notion.location

import scalaz._, Scalaz._
import com.ambiata.mundane.io._

sealed trait Location

case class HdfsLocation(path: String) extends Location {
  def dirPath  = DirPath.unsafe(path)
  def filePath = FilePath.unsafe(path)
}

object HdfsLocation {
  def create(dir: DirPath): HdfsLocation =
    HdfsLocation(dir.path)
}

case class S3Location(bucket: String, key: String) extends Location

case class LocalLocation(path: String) extends Location {
  def dirPath  = DirPath.unsafe(path)
  def filePath = FilePath.unsafe(path)
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
