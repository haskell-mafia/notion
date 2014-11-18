package com.ambiata.notion.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.TemporaryFilePath._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.core._
import com.ambiata.saws.s3.TemporaryS3._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.specs2._

class SequenceUtilSpec extends Specification with ScalaCheck { def is = s2"""

  Sequence Util should be able to read and write
  ==============================================

  from a LocalLocation              $local
  from a S3Location                 $s3                     ${tag("aws")}
  from a HdfsLocation               $hdfs

"""
  val conf = new Configuration
  val client = Clients.s3

  def local = prop((list: List[String]) => {
    withFilePath(f => {
      runTest(LocalLocation(f.path), list, Files.exists(f))
    }) must beOkValue(true -> list) })

  def s3 = prop((list: List[String]) => {
    withS3Address(s3 => {
      runTest(S3Location(s3.bucket, s3.key), list, s3.exists.executeT(client))
    }) must beOkValue(true -> list) }).set(minTestsOk = 10)

  def hdfs = prop((list: List[String] ) => {
    withFilePath(f => {
      runTest(HdfsLocation(f.path), list, Hdfs.exists(new Path(f.path)).run(conf))
    }) must beOkValue(true -> list) })

  def runTest(loc: Location, l: List[String], exists: ResultTIO[Boolean]): ResultTIO[(Boolean, List[String])]  = for {
    _ <- SequenceUtil.writeBytes(loc, conf, client, None){
      writer => ResultT.safe(l.foreach(s => writer(s.getBytes("UTF-8"))))
    }
    e <- exists
    r <- SequenceUtil.readThrift[String](loc, conf, client){
      reader => new String(reader, "UTF-8")
    }
  } yield e -> r


}
