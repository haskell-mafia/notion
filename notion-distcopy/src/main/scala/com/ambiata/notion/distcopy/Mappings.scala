package com.ambiata.notion.distcopy

import com.ambiata.poacher.hdfs._
import com.ambiata.saws.s3._

import scala.collection.JavaConverters._
import scalaz.Monoid

case class Mappings(mappings: Vector[Mapping]) {
  def toThrift: MappingsLookup =
    new MappingsLookup(mappings.toList.map({
      case UploadMapping(f, t) =>
        MappingLookup.upload(new UploadMappingLookup(f.path.path, t.bucket, t.key))
      case DownloadMapping(f, t) =>
        MappingLookup.download(new DownloadMappingLookup(f.bucket, f.key, t.path.path))
    }).asJava)

  def isEmpty: Boolean =
    mappings.isEmpty

  def distinct: Mappings =
    Mappings(mappings.distinct)

}

object Mappings {

  implicit def MappingsMonoid: Monoid[Mappings] = new Monoid[Mappings] {
    def zero: Mappings =
      Mappings.empty

    def append(m1: Mappings, m2: =>Mappings): Mappings =
      Mappings(m1.mappings ++ m2.mappings)
  }

  def empty: Mappings =
    Mappings(Vector())

  def fromThrift(m: MappingsLookup): Mappings =
    Mappings(m.mappings.asScala.toVector.map(m =>
      if (m.isSetUpload) {
        val up = m.getUpload
        UploadMapping(HdfsPath.fromString(up.path), S3Address(up.bucket, up.key))
      } else {
        val d = m.getDownload
        DownloadMapping(S3Address(d.bucket, d.key), HdfsPath.fromString(d.path))
      }
    ))
}
