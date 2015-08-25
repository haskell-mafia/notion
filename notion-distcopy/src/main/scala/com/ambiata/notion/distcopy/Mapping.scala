package com.ambiata.notion.distcopy

import com.ambiata.saws.s3.S3Address
import org.apache.hadoop.fs.Path

sealed trait Mapping {
  def render: String
}

case class DownloadMapping(from: S3Address, to: Path) extends Mapping {
  def render: String =
    s"$from,$to"
}

case class UploadMapping(from: Path, to: S3Address) extends Mapping {
  def render: String =
    s"$from,$to"
}
