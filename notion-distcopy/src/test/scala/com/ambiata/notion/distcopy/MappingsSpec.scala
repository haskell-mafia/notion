package com.ambiata.notion.distcopy

import com.ambiata.disorder._
import com.ambiata.saws.testing.Arbitraries._
import com.ambiata.saws.s3._

import org.apache.hadoop.fs.Path

import org.scalacheck._, Arbitrary._
import org.specs2._


class MappingsSpec extends Specification with ScalaCheck { def is = s2"""

 Thrift conversion is consistent

  ${ prop((m: Mappings) => Mappings.fromThrift(m.toThrift) ==== m) }

"""

  implicit def MappingsArbitrary: Arbitrary[Mappings] =
    Arbitrary(Gen.listOf(arbitrary[Mapping]).flatMap(i => Mappings(i.toVector)))

  implicit def MappingArbitrary: Arbitrary[Mapping] = Arbitrary(for {
    s <- arbitrary[S3Address]
    q <- arbitrary[NonEmptyString]
    p = new Path(q.value.replace(':', ' '))
    m <- Gen.oneOf(Gen.const(UploadMapping(p, s)), Gen.const(DownloadMapping(s, p)))
  } yield m)

}
