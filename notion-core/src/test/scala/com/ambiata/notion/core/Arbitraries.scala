package com.ambiata.notion.core

import com.ambiata.notion.core.TemporaryType._
import org.scalacheck._

object Arbitraries {
  // This is a little dodgy, but means that property tests can be run on Travis without having AWS access
  val awsEnabled = sys.env.contains("FORCE_AWS") || sys.env.contains("AWS_ACCESS_KEY")
  if (!awsEnabled) {
    println("WARNING: AWS has been disabled for this build")
  }

  implicit def TemporaryTypeArbitrary: Arbitrary[TemporaryType] = {
    Arbitrary(if (awsEnabled) Gen.oneOf(Posix, S3, Hdfs) else Gen.oneOf(Posix, Hdfs))
  }
}
