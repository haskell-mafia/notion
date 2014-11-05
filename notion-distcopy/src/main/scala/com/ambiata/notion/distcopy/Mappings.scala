package com.ambiata.notion.distcopy

import argonaut._, Argonaut._

case class Mappings(mappings: Vector[Mapping])

object Mappings {
  implicit def MappingsCodecJson: CodecJson[Mappings] =
    casecodec1(Mappings.apply, Mappings.unapply)("dist-copy-mappings")
}
