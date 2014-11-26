package com.ambiata.notion.distcopy

import com.ambiata.mundane.io._
import com.ambiata.saws.s3._
import com.ambiata.saws.testing.Arbitraries._
import org.apache.hadoop.fs.Path

import org.scalacheck._, Arbitrary._

import MemoryConversions._


case class BigData(value: String) {
  override def toString: String = {
    s"BigData failed with length: ${value.length.bytes.show}"
  }
}

object Arbitraries {
  val bigData: String = new String({
    val x = new Array[Char](1024*1024*15)
    java.util.Arrays.fill(x, 'x')
    x
  })

  implicit def BigDataArbitrary: Arbitrary[BigData] =
    Arbitrary(Gen.oneOf(
        Gen.const(bigData)
      , arbitrary[String]
    ).map(BigData))

  implicit def PathArbitrary: Arbitrary[Path] = Arbitrary(for {
    i <- Gen.choose(1, 5)
    a <- Gen.listOfN(i, Gen.identifier)
    r = a.mkString("/")
  } yield (new Path(s"/$r")))

  implicit def DownloadMappingArbitrary: Arbitrary[DownloadMapping] = Arbitrary(for {
    s <- arbitrary[S3Address]
    p <- arbitrary[Path]
  } yield DownloadMapping(s, p))

  implicit def UploadMappingArbitrary: Arbitrary[UploadMapping] = Arbitrary(for {
    p <- arbitrary[Path]
    s <- arbitrary[S3Address]
  } yield UploadMapping(p, s))

  implicit def MappingArbitrary: Arbitrary[Mapping] =
    Arbitrary(Gen.oneOf(arbitrary[DownloadMapping], arbitrary[UploadMapping]))

  implicit def MappingsArbitrary: Arbitrary[Mappings] = Arbitrary(for {
    i <- Gen.choose(1, 1000)
    a <- Gen.listOfN(i, arbitrary[Mapping])
  } yield Mappings(a.toVector))
}
