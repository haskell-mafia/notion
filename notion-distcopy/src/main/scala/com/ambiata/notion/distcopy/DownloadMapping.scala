package com.ambiata.notion.distcopy

import argonaut._, Argonaut._
import com.ambiata.com.amazonaws.services.s3.AmazonS3Client
import com.ambiata.mundane.control._
import com.ambiata.notion.distcopy.MappingOrphans._
import com.ambiata.saws.s3.{S3Address, SizedS3Address}
import org.apache.hadoop.fs.Path


case class DownloadMapping(from: SizedS3Address, to: Path)

object DownloadMapping {

  def fromS3Address(address: S3Address, to: Path, client: AmazonS3Client): ResultTIO[DownloadMapping] = for {
    size <- address.size.executeT(client)
  } yield DownloadMapping(SizedS3Address(address, size), to)

  implicit def MappingCodecJson: CodecJson[DownloadMapping] =
    casecodec2(DownloadMapping.apply, DownloadMapping.unapply)("from", "to")
}
