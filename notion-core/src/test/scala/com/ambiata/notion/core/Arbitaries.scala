package com.ambiata.notion.core

import com.ambiata.notion.core.TemporaryType._
import org.scalacheck._

object Arbitaries {
  implicit def StoreTypeArbitrary: Arbitrary[TemporaryType] =
    Arbitrary(Gen.oneOf(Posix, S3, Hdfs))
}
