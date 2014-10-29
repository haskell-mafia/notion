package com.ambiata.notion.distcopy

import argonaut._, Argonaut._


case class Workload(indexes: Vector[Int])

object Workload {
  implicit def WorkloadCodecJson: CodecJson[Workload] =
    casecodec1(Workload.apply, Workload.unapply)("index")
}
