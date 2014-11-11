package com.ambiata.notion.core

import com.ambiata.notion.core.TemporaryType._
import org.scalacheck._

object Arbitraries {
  implicit def TemporaryTypeArbitrary: Arbitrary[TemporaryType] =
    Arbitrary(Gen.oneOf(Posix, S3, Hdfs))
}
