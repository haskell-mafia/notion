package com.ambiata.notion.core

import Location._
import argonaut.CodecJson
import org.specs2.{ScalaCheck, Specification}
import scalaz._, Scalaz._
import Location._
import Arbitraries._

class LocationSpec extends Specification with ScalaCheck { def is = s2"""

 A Location can be created from a URI
   ${ fromUri("hello/world") ==== LocalLocation("hello/world").right }
   ${ fromUri("hdfs://100.100.1:9000/hello/world") ==== HdfsLocation("/hello/world").right }
   ${ fromUri("hdfs:/hello/world") ==== HdfsLocation("/hello/world").right }
   ${ fromUri("file:/hello/world") ==== LocalLocation("/hello/world").right }
   ${ fromUri("s3://hello/world") ==== S3Location("hello", "world").right }

 A Location can be serialized/deserialized to/from Json $json

"""

  def json = prop(CodecJson.derived[Location].codecLaw.encodedecode _)
}
