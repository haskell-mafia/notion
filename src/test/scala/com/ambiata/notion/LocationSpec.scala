package com.ambiata.notion

import Location._
import org.specs2.Specification
import scalaz._, Scalaz._

class LocationSpec extends Specification { def is = s2"""

 A Location can be created from a URI
   ${ fromUri("hello/world") ==== LocalLocation("hello/world").right }
   ${ fromUri("hdfs://100.100.1:9000/hello/world") ==== HdfsLocation("/hello/world").right }
   ${ fromUri("hdfs:/hello/world") ==== HdfsLocation("/hello/world").right }
   ${ fromUri("file:/hello/world") ==== LocalLocation("/hello/world").right }
   ${ fromUri("s3://hello/world") ==== S3Location("hello", "world").right }

"""
}
