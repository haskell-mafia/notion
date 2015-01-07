package com.ambiata.notion.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.Arbitraries._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.poacher.hdfs._
import com.ambiata.poacher.hdfs.Arbitraries._
import com.ambiata.saws.core._
import com.ambiata.saws.s3._
import com.ambiata.saws.testing.Arbitraries._

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

  def local = prop((list: List[String], local: LocalTemporary) => for {
    f <- local.file
    r <- runTest(LocalLocation(f.path), list, Files.exists(f))
  } yield r ==== true -> list)

  def s3 = prop((list: List[String], s3: S3Temporary) => for {
    a <- s3.address.execute(client)
    r <- runTest(S3Location(a.bucket, a.key), list, a.exists.execute(client))
  } yield r ==== true -> list)

  def hdfs = prop((list: List[String], hdfs: HdfsTemporary) => for {
    p <- hdfs.path.run(conf)
    r <- runTest(HdfsLocation(p.toString), list, Hdfs.exists(p).run(conf))
  } yield r ==== true -> list)

  def runTest(loc: Location, l: List[String], exists: RIO[Boolean]): RIO[(Boolean, List[String])]  = for {
    _ <- SequenceUtil.writeBytes(loc, conf, client, None){
      writer => RIO.safe(l.foreach(s => writer(s.getBytes("UTF-8"))))
    }
    e <- exists
    r <- SequenceUtil.readThrift[String](loc, conf, client){
      reader => new String(reader, "UTF-8")
    }
  } yield e -> r


}
