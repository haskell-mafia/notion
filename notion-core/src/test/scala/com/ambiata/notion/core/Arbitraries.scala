package com.ambiata.notion.core

import com.ambiata.mundane.io._
import com.ambiata.mundane.path._
import com.ambiata.mundane.path.Arbitraries._
import com.ambiata.notion.core.TemporaryType._
import com.ambiata.poacher.hdfs.HdfsPath
import com.ambiata.saws.s3.S3Pattern
import com.ambiata.saws.testing.Arbitraries._

import org.scalacheck._, Arbitrary._

object Arbitraries {
  // This is a little dodgy, but means that property tests can be run on Travis without having AWS access
  val awsEnabled = sys.env.contains("FORCE_AWS") || sys.env.contains("AWS_ACCESS_KEY")
  if (!awsEnabled) {
    println("WARNING: AWS has been disabled for this build")
  }

  implicit def TemporaryTypeArbitrary: Arbitrary[TemporaryType] = {
    Arbitrary(if (awsEnabled) Gen.oneOf(Posix, S3, Hdfs) else Gen.oneOf(Posix, Hdfs))
  }

  implicit def LocationArbitrary: Arbitrary[Location] = Arbitrary {
    Gen.frequency((1, arbitrary[HdfsLocation]: Gen[Location]),
      (1, arbitrary[LocalLocation]),
      (1, arbitrary[S3Location]))
  }

  implicit def LocalLocationArbitrary: Arbitrary[LocalLocation] =
    Arbitrary(arbitrary[Path].map(p => LocalLocation(LocalPath(p))))

  implicit def HdfsLocationArbitrary: Arbitrary[HdfsLocation] =
    Arbitrary(arbitrary[Path].map(p => HdfsLocation(HdfsPath(p))))

  implicit def S3LocationArbitrary: Arbitrary[S3Location] =
    Arbitrary(S3PatternArbitrary.arbitrary.map(S3Location.apply))

  def genPath: Gen[String] =
    Gen.nonEmptyListOf(Gen.identifier).map(_.mkString("/"))

}
