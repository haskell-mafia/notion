package com.ambiata.notion.core

import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs._
import com.ambiata.saws.s3._
import Location._

import Arbitraries._
import argonaut.CodecJson
import org.specs2.{ScalaCheck, Specification}

import scalaz._, Scalaz._

class LocationSpec extends Specification with ScalaCheck { def is = s2"""

 A Location can be created from a URI
   ${ fromUri("hello/world") ==== LocalLocation(LocalPath.fromString("hello/world")).right }
   ${ fromUri("hdfs://100.100.1:9000/hello/world") ==== HdfsLocation(HdfsPath.fromString("/hello/world")).right }
   ${ fromUri("hdfs:/hello/world") ==== HdfsLocation(HdfsPath.fromString("/hello/world")).right }
   ${ fromUri("file:/hello/world") ==== LocalLocation(LocalPath.fromString("/hello/world")).right }
   ${ fromUri("s3://hello/world") ==== S3Location(S3Pattern("hello", "world")).right }

 A Local location can be created from a URI
   ${ localLocationFromUri("hello/world") ==== LocalLocation(LocalPath.fromString("hello/world")).right }
   ${ localLocationFromUri("file:/hello/world") ==== LocalLocation(LocalPath.fromString("/hello/world")).right }
   ${ localLocationFromUri("s3://hello/world").toEither must beLeft }
   ${ localLocationFromUri("hdfs:/hello/world").toEither must beLeft }

 A S3 location can be created from a URI
   ${ s3LocationFromUri("s3://hello/world") ==== S3Location(S3Pattern("hello", "world")).right }
   ${ s3LocationFromUri("hdfs:/hello/world").toEither must beLeft }
   ${ s3LocationFromUri("file:/hello/world").toEither must beLeft }
   ${ s3LocationFromUri("hello/world").toEither must beLeft }

 A HDFS location can be created from a URI
   ${ hdfsLocationFromUri("hdfs:/hello/world") ==== HdfsLocation(HdfsPath.fromString("/hello/world")).right }
   ${ hdfsLocationFromUri("/hello/world").toEither must beLeft }
   ${ hdfsLocationFromUri("file:/hello/world").toEither must beLeft }
   ${ hdfsLocationFromUri("s3:/hello/world").toEither must beLeft }

 A Location can be serialized/deserialized to/from Json $json

"""

  def json = prop(CodecJson.derived[Location].codecLaw.encodedecode _)
}
