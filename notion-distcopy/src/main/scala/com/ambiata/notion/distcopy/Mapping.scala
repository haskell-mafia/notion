package com.ambiata.notion.distcopy

import com.ambiata.saws.s3.S3Address
import org.apache.hadoop.fs.Path

sealed trait Mapping

case class DownloadMapping(from: S3Address, to: Path) extends Mapping
case class UploadMapping(from: Path, to: S3Address) extends Mapping
