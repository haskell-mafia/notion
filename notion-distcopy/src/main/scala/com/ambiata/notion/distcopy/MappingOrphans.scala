package com.ambiata.notion.distcopy

import argonaut._, Argonaut._
import com.ambiata.saws.s3.{SizedS3Address, S3Address}
import org.apache.hadoop.fs.Path

object MappingOrphans {

  implicit def S3AddressCodecJson: CodecJson[S3Address] =
    casecodec2(S3Address.apply, S3Address.unapply)("bucket", "key")

  implicit def SizedS3AddressCodecJson: CodecJson[SizedS3Address] =
    casecodec2(SizedS3Address.apply, SizedS3Address.unapply)("address", "size")

  implicit def HdfsPathCodecJson: CodecJson[Path] =
    CodecJson(_.toString.asJson, _.as[String].map(new Path(_)))

}
