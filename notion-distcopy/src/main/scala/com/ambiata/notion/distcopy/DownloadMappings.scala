package com.ambiata.notion.distcopy

import argonaut._, Argonaut._

case class DownloadMappings(mappings: Vector[DownloadMapping])

object DownloadMappings {
  implicit def MappingsCodecJson: CodecJson[DownloadMappings] =
    casecodec1(DownloadMappings.apply, DownloadMappings.unapply)("download-mappings")
}
