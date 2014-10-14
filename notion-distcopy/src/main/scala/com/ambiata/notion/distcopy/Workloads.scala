package com.ambiata.notion.distcopy

import argonaut._, Argonaut._

case class Workloads(workloads: Vector[Workload])

object Workloads {
  implicit def WorkloadsCodecJson: CodecJson[Workloads] =
    casecodec1(Workloads.apply, Workloads.unapply)("workloads")
}
