package com.ambiata.notion.distcopy

import com.ambiata.saws.s3._

import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._

case class Mappings(mappings: Vector[Mapping]) {
  def toThrift: MappingsLookup =
    new MappingsLookup(mappings.toList.map({
      case UploadMapping(f, t) =>
        MappingLookup.upload(new UploadMappingLookup(f.toString, t.bucket, t.key))
      case DownloadMapping(f, t) =>
        MappingLookup.download(new DownloadMappingLookup(f.bucket, f.key, t.toString))
    }).asJava)

}

object Mappings {
  def fromThrift(m: MappingsLookup): Mappings =
    Mappings(m.mappings.asScala.toVector.map(m =>
      if (m.isSetUpload) {
        val up = m.getUpload
        UploadMapping(new Path(up.path), S3Address(up.bucket, up.key))
      } else {
        val d = m.getDownload
        DownloadMapping(S3Address(d.bucket, d.key), new Path(d.path))
      }
    ))
}
