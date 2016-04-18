package com.ambiata.notion.distcopy

import com.ambiata.poacher.hdfs.HdfsPath
import com.ambiata.saws.s3.S3Address

sealed trait Mapping {
  def render: String
}

case class DownloadMapping(from: S3Address, to: HdfsPath) extends Mapping {
  def render: String =
    s"s3://${from.bucket}/${from.key},hdfs://${to.path.path}"
}

case class UploadMapping(from: HdfsPath, to: S3Address) extends Mapping {
  def render: String =
    s"hdfs://${from.path.path},s3://${to.bucket}/${to.key}"
}
