package com.ambiata.notion.distcopy

import com.ambiata.mundane.io._

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


  implicit def ArbitraryFilePath: Arbitrary[FilePath] =
    Arbitrary {
      for {
        n  <- Gen.choose(1, 3)
        fs <- Gen.listOfN(n, Gen.identifier).map(fs => FilePath.unsafe(fs.mkString("/")))
      } yield fs
    }
}
