package com.ambiata.notion.distcopy

import argonaut._, Argonaut._
import com.ambiata.notion.distcopy.MappingOrphans._
import com.ambiata.saws.s3.{S3Address, SizedS3Address}
import org.apache.hadoop.fs.Path

import scalaz._, Scalaz._

sealed trait Mapping
case class DownloadMapping(from: S3Address, to: Path) extends Mapping
case class UploadMapping(from: Path, to: S3Address) extends Mapping

object Mapping {
  implicit def DownloadMappingCodecJson: CodecJson[DownloadMapping] =
    casecodec2(DownloadMapping.apply, DownloadMapping.unapply)("from", "to")

  implicit def UploadMappingCodecJson: CodecJson[UploadMapping] =
    casecodec2(UploadMapping.apply, UploadMapping.unapply)("from", "to")

  implicit def MappingCodecJson: CodecJson[Mapping] =
    CodecJson({
      case DownloadMapping(f, t) => Json("mapping" := "download", "from" := f, "to" := t)
      case UploadMapping(f, t) => Json("mapping" := "upload", "from" := f, "to" := t)
    }, c => for {
      mapping <- (c --\ "mapping").as[String]
      x <- if (mapping == "download")
             ((c --\ "from").as[S3Address] |@| (c --\ "to").as[Path]).apply(DownloadMapping)
           else
             ((c --\ "from").as[Path] |@| (c --\ "to").as[S3Address]).apply(UploadMapping)
    } yield x)

}
